import sqlite3
conn = sqlite3.connect('/root/telegram-bot/freqtrade_instance_ru/freqtrade_test/user_data/tradesv3_ru.sqlite')
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(trades)")
for col in cursor.fetchall():
    print(f"  {col[1]} ({col[2]})")
print()
cursor.execute('SELECT COUNT(*) FROM trades')
print('Trades count:', cursor.fetchone()[0])
cursor.execute('SELECT * FROM trades ORDER BY open_date DESC LIMIT 20')
for row in cursor.fetchall():
    print(row)
    print('---')
conn.close()
