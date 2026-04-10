import sqlite3
conn = sqlite3.connect('tradesv3.dryrun.sqlite')
cursor = conn.cursor()

for table in ['KeyValueStore']:
    cursor.execute(f"SELECT * FROM {table}")
    print(f"=== {table} ===")
    for row in cursor.fetchall():
        print(row)

print("\n=== Checking if any trades exist ===")
cursor.execute("SELECT COUNT(*) FROM trades")
print("Trades count:", cursor.fetchone()[0])

cursor.execute("SELECT * FROM trades LIMIT 10")
print("\n=== trades ===")
for row in cursor.fetchall():
    print(row)

conn.close()
