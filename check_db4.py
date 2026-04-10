import sqlite3
import json
conn = sqlite3.connect('data/maria_history.db')
cursor = conn.cursor()

cursor.execute("SELECT * FROM maria_history LIMIT 5")
rows = cursor.fetchall()
for row in rows:
    print(f"user_id: {row[0]}")
    try:
        history = json.loads(row[1])
        print(f"  history entries: {len(history)}")
        if history:
            print(f"  first entry: {history[0]}")
    except:
        print(f"  raw: {row[1][:200]}...")
    print()

conn.close()
