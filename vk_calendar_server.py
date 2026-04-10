import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv("VK_RENT_DB_PATH", str(BASE_DIR / "rent_bot.db")))
HOST = os.getenv("VK_CALENDAR_HOST", "0.0.0.0")
PORT = int(os.getenv("VK_CALENDAR_PORT", "8090"))
DEFAULT_DAYS = int(os.getenv("VK_CALENDAR_DEFAULT_DAYS", "14"))


def _parse_ts(ts: str) -> datetime:
    raw = str(ts or "").strip().replace("Z", "+00:00")
    if not raw:
        return datetime(1970, 1, 1, 0, 0, 0)
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _load_bookings(start_dt: datetime, end_dt: datetime) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            ORDER BY start_ts
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
        out: list[dict] = []
        for r in rows:
            s = _parse_ts(r["start_ts"])
            e = _parse_ts(r["end_ts"])
            out.append(
                {
                    "id": int(r["id"]),
                    "user_id": int(r["user_id"]),
                    "start_ts": s.isoformat(),
                    "end_ts": e.isoformat(),
                    "duration_minutes": int((e - s).total_seconds() // 60),
                }
            )
        return out
    finally:
        conn.close()


def _calendar_html() -> str:
    return """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Календарь занятости кабинета</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 20px; color: #222; }
    h1 { margin: 0 0 8px; }
    .muted { color: #666; margin-bottom: 14px; }
    .controls { display: flex; gap: 8px; align-items: center; margin-bottom: 16px; }
    .controls input { padding: 6px 8px; }
    .controls button { padding: 6px 10px; cursor: pointer; }
    .day { border: 1px solid #ddd; border-radius: 8px; margin: 12px 0; overflow: hidden; }
    .day-head { background: #f6f6f6; padding: 10px 12px; font-weight: 700; }
    .day-body { padding: 10px 12px; }
    .slot { padding: 6px 8px; margin: 6px 0; border-radius: 6px; background: #ffe6e6; border: 1px solid #ffcccc; }
    .free { color: #1d7a2f; }
    code { background: #f1f1f1; padding: 2px 6px; border-radius: 4px; }
  </style>
</head>
<body>
  <h1>Календарь занятости кабинета</h1>
  <div class="muted">Источник: <code>rent_bot.db</code>. Время отображается в UTC.</div>
  <div class="controls">
    <label>Период (дней): <input id="days" type="number" min="1" max="60" value="14"></label>
    <button id="reload">Обновить</button>
  </div>
  <div id="calendar"></div>
<script>
function pad(n){ return String(n).padStart(2,'0'); }
function fmtDt(iso){
  const d = new Date(iso + "Z");
  return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;
}
function fmtDate(iso){
  const d = new Date(iso + "Z");
  return `${pad(d.getUTCDate())}.${pad(d.getUTCMonth()+1)}.${d.getUTCFullYear()}`;
}
function dayKey(iso){
  const d = new Date(iso + "Z");
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`;
}
function render(data){
  const el = document.getElementById("calendar");
  el.innerHTML = "";
  const byDay = new Map();
  for (const b of data.bookings){
    const key = dayKey(b.start_ts);
    if (!byDay.has(key)) byDay.set(key, []);
    byDay.get(key).push(b);
  }
  const start = new Date(data.start_ts + "Z");
  for(let i=0; i<data.days; i++){
    const d = new Date(start.getTime() + i*24*60*60*1000);
    const key = `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`;
    const dayBookings = byDay.get(key) || [];
    const card = document.createElement("div");
    card.className = "day";
    const head = document.createElement("div");
    head.className = "day-head";
    head.textContent = `${pad(d.getUTCDate())}.${pad(d.getUTCMonth()+1)}.${d.getUTCFullYear()} (UTC)`;
    card.appendChild(head);
    const body = document.createElement("div");
    body.className = "day-body";
    if (!dayBookings.length){
      const p = document.createElement("div");
      p.className = "free";
      p.textContent = "Свободно весь день";
      body.appendChild(p);
    } else {
      dayBookings.sort((a,b)=>a.start_ts.localeCompare(b.start_ts));
      for (const b of dayBookings){
        const s = fmtDt(b.start_ts);
        const e = fmtDt(b.end_ts);
        const row = document.createElement("div");
        row.className = "slot";
        row.textContent = `${s}–${e} · занято`;
        body.appendChild(row);
      }
    }
    card.appendChild(body);
    el.appendChild(card);
  }
}
async function load(){
  const days = Number(document.getElementById("days").value || 14);
  const r = await fetch(`/api/slots?days=${days}`);
  const data = await r.json();
  render(data);
}
document.getElementById("reload").addEventListener("click", load);
load();
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, body: str, status: int = 200) -> None:
        raw = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/calendar"):
            self._send_html(_calendar_html())
            return
        if parsed.path == "/health":
            ok = DB_PATH.exists()
            self._send_json({"ok": ok, "db_path": str(DB_PATH)})
            return
        if parsed.path == "/api/slots":
            q = parse_qs(parsed.query or "")
            try:
                days = int((q.get("days") or [str(DEFAULT_DAYS)])[0])
            except ValueError:
                days = DEFAULT_DAYS
            days = max(1, min(days, 60))
            start = datetime.now(timezone.utc).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=days)
            bookings = _load_bookings(start, end)
            self._send_json(
                {
                    "days": days,
                    "start_ts": start.isoformat(),
                    "end_ts": end.isoformat(),
                    "bookings": bookings,
                }
            )
            return
        self._send_json({"error": "not found"}, status=404)

    def log_message(self, fmt: str, *args) -> None:  # quieter logs
        return


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"VK calendar server started on http://{HOST}:{PORT} (db={DB_PATH})")
    server.serve_forever()


if __name__ == "__main__":
    main()

