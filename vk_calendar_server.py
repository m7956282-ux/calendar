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


def _ensure_bookings_guest_name_column(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(bookings)")
    cols = {row[1] for row in cur.fetchall()}
    if "guest_name" not in cols:
        cur.execute("ALTER TABLE bookings ADD COLUMN guest_name TEXT")
        conn.commit()


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
        _ensure_bookings_guest_name_column(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, start_ts, end_ts, guest_name
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
            gn = (r["guest_name"] or "").strip()
            out.append(
                {
                    "id": int(r["id"]),
                    "user_id": int(r["user_id"]),
                    "start_ts": s.isoformat(),
                    "end_ts": e.isoformat(),
                    "duration_minutes": int((e - s).total_seconds() // 60),
                    "guest_name": gn,
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
    :root{
      --bg:#f5f6fb;
      --card:#ffffff;
      --text:#1b1f31;
      --muted:#6f7894;
      --line:#e5e9f4;
      --event:#dfe7ff;
      --event-border:#7b8fff;
      --hour-h:52px;
    }
    *{box-sizing:border-box;}
    body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;}
    .wrap{max-width:1320px;margin:0 auto;padding:14px;}
    h1{margin:0 0 6px;font-size:28px;font-weight:700;}
    .muted{color:var(--muted);margin-bottom:10px;}
    .controls{
      display:flex;gap:8px;align-items:end;flex-wrap:wrap;
      background:var(--card);border:1px solid var(--line);border-radius:12px;
      padding:10px;margin-bottom:10px;
    }
    .controls label{display:grid;gap:4px;font-size:12px;color:#556089;}
    .controls input,.controls button{
      padding:8px 10px;border-radius:10px;border:1px solid #d5dcef;background:#fff;
    }
    .controls button{cursor:pointer;font-weight:600;}
    .btn-primary{background:#1f6fff;color:#fff;border-color:#1f6fff;}
    .btn-secondary{background:#fff;color:#334; }
    .view-switch{display:flex;gap:6px;}
    .view-switch button.active{background:#1f6fff;color:#fff;border-color:#1f6fff;}
    .address{font-size:14px;color:#334; margin: 6px 0 10px;}
    .address b{font-weight:700;}
    .calendar-shell{
      background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:auto;
      scrollbar-width:none;-ms-overflow-style:none;
    }
    .calendar-shell::-webkit-scrollbar{display:none;height:0;width:0;}
    .week{min-width:980px;border-top:1px solid var(--line);}
    .week:first-child{border-top:none;}
    .week-title{padding:10px 12px;border-bottom:1px solid var(--line);font-weight:600;color:#4c5677;background:#fafbff;}
    .grid-head{display:grid;grid-template-columns:64px repeat(7,1fr);border-bottom:1px solid var(--line);position:sticky;top:0;background:#fff;z-index:2;}
    .grid-head .corner{border-right:1px solid var(--line);}
    .day-col-head{padding:8px 6px;border-right:1px solid var(--line);text-align:center;}
    .day-col-head:last-child{border-right:none;}
    .day-col-head .d{font-size:13px;color:var(--muted);}
    .day-col-head .n{font-size:16px;font-weight:700;}
    .day-col-head.today{background:#f0f5ff;}
    .grid-body{position:relative;display:grid;grid-template-columns:64px repeat(7,1fr);height:calc(var(--hour-h) * 24);}
    .times{position:relative;border-right:1px solid var(--line);}
    .time-label{position:absolute;right:8px;transform:translateY(-50%);font-size:11px;color:#9aa3bf;}
    .day-col{position:relative;border-right:1px solid var(--line);}
    .day-col:last-child{border-right:none;}
    .line-hour{position:absolute;left:0;right:0;border-top:1px solid #e9edf7;}
    .line-quarter{position:absolute;left:0;right:0;border-top:1px dashed #f1f4fb;}
    .event{
      position:absolute;left:6px;right:6px;
      background:var(--event);border:1px solid var(--event-border);
      border-radius:10px;padding:4px 6px;
      overflow-x:hidden;overflow-y:auto;
      scrollbar-width:thin;
      box-shadow:0 2px 6px rgba(69,93,187,.15);
    }
    .event .t{
      font-size:12px;font-weight:700;
      line-height:1.2;
      overflow-wrap:anywhere;word-break:break-word;
    }
    .event .s{
      font-size:11px;color:#4f5b85;margin-top:2px;
      line-height:1.25;
      overflow-wrap:anywhere;word-break:break-word;
      hyphens:auto;
    }
    .empty-day{position:absolute;left:8px;right:8px;top:8px;font-size:12px;color:#98a3c2;}
    @media (max-width: 760px){
      h1{font-size:22px;}
      .wrap{padding:10px;}
      .controls{display:grid;grid-template-columns:1fr 1fr;}
      .view-switch{grid-column:1/-1;}
      .calendar-shell{border-radius:10px;}
      .week{min-width:860px;}
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Календарь занятости кабинета</h1>
    <div class="muted">Недельная сетка, шаг 15 минут.</div>
    <div class="address"><b>Адрес кабинета:</b> Пушкинская 268</div>
    <div class="controls">
      <label>С даты <input id="dateFrom" type="date"></label>
      <label>По дату <input id="dateTo" type="date"></label>
      <button id="prevBtn" class="btn-secondary">←</button>
      <button id="nextBtn" class="btn-secondary">→</button>
      <button id="todayBtn" class="btn-secondary">Сегодня</button>
      <button id="applyBtn" class="btn-primary">Применить</button>
      <div class="view-switch">
        <button id="viewWeek" class="btn-secondary">Неделя</button>
        <button id="view2Week" class="btn-secondary">2 недели</button>
        <button id="viewMonth" class="btn-secondary">Месяц</button>
      </div>
    </div>
    <div id="selectedRange" class="muted"></div>
    <div id="calendar" class="calendar-shell"></div>
  </div>
<script>
const HOUR_H = 52;
let currentPreset = 14;
function pad(n){ return String(n).padStart(2,'0'); }
function escapeHtml(s){
  const t = String(s ?? '');
  return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function isoDateUTC(d){ return `${d.getUTCFullYear()}-${pad(d.getUTCMonth()+1)}-${pad(d.getUTCDate())}`; }
function parseIso(iso){ return new Date(iso + "Z"); }
function parseDateInput(v){
  if(!v) return null;
  const [y,m,d] = v.split("-").map(Number);
  return new Date(Date.UTC(y,m-1,d));
}
function dayKeyFromDate(d){ return isoDateUTC(d); }
function minutesFromStartOfDay(d){ return d.getUTCHours()*60 + d.getUTCMinutes(); }
function weekStart(date){
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const w = d.getUTCDay();
  const delta = (w + 6) % 7;
  d.setUTCDate(d.getUTCDate() - delta);
  return d;
}
function fmtWeekTitle(start){
  const end = new Date(start.getTime() + 6*24*3600*1000);
  return `${pad(start.getUTCDate())}.${pad(start.getUTCMonth()+1)}.${start.getUTCFullYear()} - ${pad(end.getUTCDate())}.${pad(end.getUTCMonth()+1)}.${end.getUTCFullYear()}`;
}
function weekdayShort(i){ return ["пн","вт","ср","чт","пт","сб","вс"][i]; }
function setPreset(days){
  currentPreset = days;
  const b1 = document.getElementById("viewWeek");
  const b2 = document.getElementById("view2Week");
  const b3 = document.getElementById("viewMonth");
  [b1,b2,b3].forEach(b=>b.classList.remove("active"));
  if(days===7) b1.classList.add("active");
  else if(days===14) b2.classList.add("active");
  else b3.classList.add("active");
}
function syncDateRange(days){
  const from = parseDateInput(document.getElementById("dateFrom").value) || new Date();
  const start = new Date(Date.UTC(from.getUTCFullYear(), from.getUTCMonth(), from.getUTCDate()));
  const end = new Date(start.getTime() + (days-1)*24*3600*1000);
  document.getElementById("dateFrom").value = isoDateUTC(start);
  document.getElementById("dateTo").value = isoDateUTC(end);
}
function applyToday(){
  const t = new Date();
  document.getElementById("dateFrom").value = isoDateUTC(t);
  syncDateRange(currentPreset);
}
function shiftRange(direction){
  const from = parseDateInput(document.getElementById("dateFrom").value);
  const to = parseDateInput(document.getElementById("dateTo").value);
  if(!from || !to) return;
  const spanDays = Math.max(1, Math.floor((to - from) / (24*3600*1000)) + 1);
  const step = spanDays * direction;
  const newFrom = new Date(from.getTime() + step * 24*3600*1000);
  const newTo = new Date(to.getTime() + step * 24*3600*1000);
  document.getElementById("dateFrom").value = isoDateUTC(newFrom);
  document.getElementById("dateTo").value = isoDateUTC(newTo);
}
function buildHeadRow(weekStartDate){
  const row = document.createElement("div");
  row.className = "grid-head";
  const corner = document.createElement("div");
  corner.className = "corner";
  row.appendChild(corner);
  const todayKey = dayKeyFromDate(new Date());
  for(let i=0;i<7;i++){
    const d = new Date(weekStartDate.getTime() + i*24*3600*1000);
    const cell = document.createElement("div");
    cell.className = "day-col-head";
    if (dayKeyFromDate(d) === todayKey) cell.classList.add("today");
    cell.innerHTML = `<div class="d">${weekdayShort(i)}</div><div class="n">${pad(d.getUTCDate())}.${pad(d.getUTCMonth()+1)}</div>`;
    row.appendChild(cell);
  }
  return row;
}
function buildBody(weekStartDate, byDay){
  const body = document.createElement("div");
  body.className = "grid-body";
  const times = document.createElement("div");
  times.className = "times";
  for(let h=0; h<=24; h++){
    const y = h * HOUR_H;
    const l = document.createElement("div");
    l.className = "time-label";
    l.style.top = `${y}px`;
    l.textContent = `${pad(h%24)}:00`;
    times.appendChild(l);
  }
  body.appendChild(times);
  for(let i=0;i<7;i++){
    const d = new Date(weekStartDate.getTime() + i*24*3600*1000);
    const key = dayKeyFromDate(d);
    const dayCol = document.createElement("div");
    dayCol.className = "day-col";
    for(let q=0; q<=96; q++){
      const line = document.createElement("div");
      line.className = (q % 4 === 0) ? "line-hour" : "line-quarter";
      line.style.top = `${(q/4) * HOUR_H}px`;
      dayCol.appendChild(line);
    }
    const list = (byDay.get(key) || []).sort((a,b)=>a.start_ts.localeCompare(b.start_ts));
    if (!list.length){
      const empty = document.createElement("div");
      empty.className = "empty-day";
      empty.textContent = "Свободно";
      dayCol.appendChild(empty);
    } else {
      for (const b of list){
        const s = parseIso(b.start_ts);
        const e = parseIso(b.end_ts);
        const startMin = minutesFromStartOfDay(s);
        const endMin = minutesFromStartOfDay(e);
        const top = (startMin / 60) * HOUR_H;
        const height = Math.max(18, ((endMin - startMin) / 60) * HOUR_H);
        const event = document.createElement("div");
        event.className = "event";
        event.style.top = `${top}px`;
        event.style.height = `${height}px`;
        const guest = (b.guest_name || '').trim();
        const sub = escapeHtml(guest || 'Занято');
        const label = guest ? guest : 'Занято';
        event.title = label;
        event.innerHTML = `<div class="t">${pad(s.getUTCHours())}:${pad(s.getUTCMinutes())} - ${pad(e.getUTCHours())}:${pad(e.getUTCMinutes())}</div><div class="s">${sub}</div>`;
        dayCol.appendChild(event);
      }
    }
    body.appendChild(dayCol);
  }
  return body;
}
function render(data){
  const root = document.getElementById("calendar");
  root.innerHTML = "";
  const selectedRange = document.getElementById("selectedRange");
  const s = parseIso(data.start_ts);
  const e = new Date(parseIso(data.end_ts).getTime() - 1000);
  selectedRange.textContent =
    `Период: ${pad(s.getUTCDate())}.${pad(s.getUTCMonth()+1)}.${s.getUTCFullYear()} - ` +
    `${pad(e.getUTCDate())}.${pad(e.getUTCMonth()+1)}.${e.getUTCFullYear()}`;
  const byDay = new Map();
  for (const b of data.bookings){
    const key = dayKeyFromDate(parseIso(b.start_ts));
    if (!byDay.has(key)) byDay.set(key, []);
    byDay.get(key).push(b);
  }
  const start = parseIso(data.start_ts);
  const startWeek = weekStart(start);
  const end = parseIso(data.end_ts);
  for(let ws = new Date(startWeek); ws < end; ws = new Date(ws.getTime() + 7*24*3600*1000)){
    const week = document.createElement("section");
    week.className = "week";
    const title = document.createElement("div");
    title.className = "week-title";
    title.textContent = fmtWeekTitle(ws);
    week.appendChild(title);
    week.appendChild(buildHeadRow(ws));
    week.appendChild(buildBody(ws, byDay));
    root.appendChild(week);
  }
}
async function load(){
  const from = parseDateInput(document.getElementById("dateFrom").value);
  const to = parseDateInput(document.getElementById("dateTo").value);
  if(!from || !to){
    applyToday();
  }
  const fromVal = document.getElementById("dateFrom").value;
  const toVal = document.getElementById("dateTo").value;
  const fromDt = parseDateInput(fromVal);
  const toDt = parseDateInput(toVal);
  if(!fromDt || !toDt || toDt < fromDt){
    alert("Проверьте диапазон дат: дата окончания должна быть не раньше даты начала.");
    return;
  }
  const r = await fetch(`/api/slots?from=${fromVal}&to=${toVal}`);
  const data = await r.json();
  render(data);
}
document.getElementById("applyBtn").addEventListener("click", load);
document.getElementById("prevBtn").addEventListener("click", () => { shiftRange(-1); load(); });
document.getElementById("nextBtn").addEventListener("click", () => { shiftRange(1); load(); });
document.getElementById("todayBtn").addEventListener("click", () => { applyToday(); load(); });
document.getElementById("viewWeek").addEventListener("click", () => { setPreset(7); syncDateRange(7); load(); });
document.getElementById("view2Week").addEventListener("click", () => { setPreset(14); syncDateRange(14); load(); });
document.getElementById("viewMonth").addEventListener("click", () => { setPreset(31); syncDateRange(31); load(); });
setPreset(14);
applyToday();
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
            start = None
            end = None
            from_raw = (q.get("from") or [None])[0]
            to_raw = (q.get("to") or [None])[0]
            if from_raw and to_raw:
                try:
                    y1, m1, d1 = [int(x) for x in from_raw.split("-")]
                    y2, m2, d2 = [int(x) for x in to_raw.split("-")]
                    start = datetime(y1, m1, d1, 0, 0, 0)
                    end = datetime(y2, m2, d2, 23, 59, 59) + timedelta(seconds=1)
                except Exception:
                    start = None
                    end = None
            if start is None or end is None or end <= start:
                try:
                    days = int((q.get("days") or [str(DEFAULT_DAYS)])[0])
                except ValueError:
                    days = DEFAULT_DAYS
                days = max(1, min(days, 120))
                start = datetime.now(timezone.utc).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
                end = start + timedelta(days=days)
            else:
                days = max(1, min(120, int((end - start).total_seconds() // 86400)))
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

