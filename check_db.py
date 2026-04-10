import sqlite3
conn = sqlite3.connect('tradesv3.dryrun.sqlite')
cursor = conn.cursor()

for table in ['KeyValueStore', 'trades', 'pairlocks', 'trade_custom_data', 'orders']:
    print(f"\n=== {table} ===")
    cursor.execute(f"PRAGMA table_info({table})")
    cols = cursor.fetchall()
    print("Columns:", [c[1] for c in cols])
    
    cursor.execute(f"SELECT * FROM {table} LIMIT 5")
    rows = cursor.fetchall()
    print(f"Rows (first 5 of {cursor.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]}):")
    for row in rows:
        print(row)

conn.close()
