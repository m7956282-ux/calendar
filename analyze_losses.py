import sqlite3

conn = sqlite3.connect('/root/telegram-bot/freqtrade_instance_ru/freqtrade_test/tradesv3.dryrun.sqlite')
cursor = conn.cursor()

# 1. Общая статистика
cursor.execute('SELECT COUNT(*) FROM trades WHERE is_open = 0')
closed = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM trades WHERE is_open = 0 AND close_profit > 0')
wins = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM trades WHERE is_open = 0 AND close_profit <= 0')
losses = cursor.fetchone()[0]

print(f"=== ОБЩАЯ СТАТИСТИКА ===")
print(f"Закрытых сделок: {closed}")
print(f"Прибыльных: {wins} ({wins/closed*100:.1f}%)")
print(f"Убыточных: {losses} ({losses/closed*100:.1f}%)")

cursor.execute('SELECT AVG(close_profit) FROM trades WHERE is_open = 0')
avg_profit = cursor.fetchone()[0]
print(f"Средняя прибыль: {avg_profit*100:.2f}%")

cursor.execute('SELECT AVG(close_profit) FROM trades WHERE is_open = 0 AND close_profit > 0')
avg_win = cursor.fetchone()[0]
print(f"Средняя прибыль (win): {avg_win*100:.2f}%")

cursor.execute('SELECT AVG(close_profit) FROM trades WHERE is_open = 0 AND close_profit <= 0')
avg_loss = cursor.fetchone()[0]
print(f"Средний убыток (loss): {avg_loss*100:.2f}%")

cursor.execute('SELECT SUM(close_profit_abs) FROM trades WHERE is_open = 0')
total_pnl = cursor.fetchone()[0]
print(f"Общий PnL: {total_pnl:.2f} USDT")

# 2. Причины выхода
print(f"\n=== ПРИЧИНЫ ВЫХОДА ===")
cursor.execute('''
    SELECT exit_reason, COUNT(*) as cnt, 
           AVG(close_profit) as avg_p, 
           SUM(close_profit_abs) as total_pnl
    FROM trades WHERE is_open = 0 
    GROUP BY exit_reason 
    ORDER BY cnt DESC
''')
for row in cursor.fetchall():
    print(f"  {row[0]:20s} | сделок: {row[1]:3d} | ср.прибыль: {row[2]*100:6.2f}% | PnL: {row[3]:8.2f} USDT")

# 3. Long vs Short
print(f"\n=== LONG vs SHORT ===")
cursor.execute('''
    SELECT 
        CASE WHEN is_short = 0 THEN 'LONG' ELSE 'SHORT' END as side,
        COUNT(*) as cnt,
        SUM(CASE WHEN close_profit > 0 THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN close_profit <= 0 THEN 1 ELSE 0 END) as losses,
        AVG(close_profit) as avg_p,
        SUM(close_profit_abs) as total_pnl
    FROM trades WHERE is_open = 0
    GROUP BY is_short
''')
for row in cursor.fetchall():
    win_rate = row[2]/row[1]*100
    print(f"  {row[0]:6s} | сделок: {row[1]:3d} | win: {row[2]:3d} | loss: {row[3]:3d} | winrate: {win_rate:5.1f}% | ср: {row[4]*100:6.2f}% | PnL: {row[5]:8.2f}")

# 4. Время удержания
print(f"\n=== ВРЕМЯ УДЕРЖИВАНИЯ ===")
cursor.execute('''
    SELECT 
        exit_reason,
        COUNT(*) as cnt,
        AVG((julianday(close_date) - julianday(open_date)) * 24 * 60) as avg_min
    FROM trades WHERE is_open = 0
    GROUP BY exit_reason
''')
for row in cursor.fetchall():
    print(f"  {row[0]:20s} | сделок: {row[1]:3d} | ср.время: {row[2]:6.1f} мин ({row[2]/60:.1f} ч)")

# 5. Все убыточные сделки
print(f"\n=== ТОП-15 УБЫТОЧНЫХ СДЕЛОК ===")
cursor.execute('''
    SELECT id, pair, open_rate, close_rate, open_date, close_date, 
           close_profit, close_profit_abs, exit_reason, enter_tag, stake_amount, is_short
    FROM trades WHERE is_open = 0 AND close_profit < 0
    ORDER BY close_profit_abs ASC
    LIMIT 15
''')
for row in cursor.fetchall():
    side = 'SHORT' if row[11] else 'LONG'
    print(f"  #{row[0]:3d} {row[1]:20s} {side:6s} | {row[6]*100:6.2f}% | {row[7]:8.2f}$ | {row[8]:15s} | вход:{row[2]} выход:{row[3]}")

# 6. Потери по парам
print(f"\n=== УБЫТКИ ПО ПАРАМ ===")
cursor.execute('''
    SELECT pair, COUNT(*) as cnt, SUM(close_profit_abs) as total_pnl, AVG(close_profit) as avg_p
    FROM trades WHERE is_open = 0 AND close_profit < 0
    GROUP BY pair
    ORDER BY total_pnl ASC
    LIMIT 15
''')
for row in cursor.fetchall():
    print(f"  {row[0]:20s} | сделок: {row[1]:3d} | убыток: {row[2]:8.2f} USDT | ср: {row[3]*100:6.2f}%")

# 7. Почасовой анализ
print(f"\n=== УБЫТКИ ПО ЧАСАМ ОТКРЫТИЯ ===")
cursor.execute('''
    SELECT 
        CAST(strftime('%H', open_date) AS INTEGER) as hour,
        COUNT(*) as cnt,
        SUM(close_profit_abs) as total_pnl,
        AVG(close_profit) as avg_p
    FROM trades WHERE is_open = 0
    GROUP BY hour
    ORDER BY hour
''')
for row in cursor.fetchall():
    print(f"  {row[0]:02d}:00 | сделок: {row[1]:3d} | PnL: {row[2]:8.2f} USDT | ср: {row[3]*100:6.2f}%")

# 8. Серия убыточных сделок
print(f"\n=== СЕРИИ УБЫТКОВ ===")
cursor.execute('''
    SELECT id, close_profit, exit_reason, pair, close_date
    FROM trades WHERE is_open = 0
    ORDER BY close_date ASC
''')
rows = cursor.fetchall()
consecutive_losses = 0
max_losses = 0
max_loss_start = None
current_loss_start = None
for row in rows:
    if row[1] <= 0:
        consecutive_losses += 1
        if consecutive_losses == 1:
            current_loss_start = row
        if consecutive_losses > max_losses:
            max_losses = consecutive_losses
            max_loss_start = current_loss_start
    else:
        consecutive_losses = 0
print(f"  Максимальная серия убытков подряд: {max_losses}")

conn.close()
