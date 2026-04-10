import sqlite3

dbs = [
    '/root/telegram-bot/freqtrade_instance_ru/freqtrade_test/tradesv3_optimized.sqlite',
    '/root/telegram-bot/freqtrade_instance_ru/freqtrade_test/tradesv3.sqlite',
    '/root/telegram-bot/freqtrade_instance_ru/freqtrade_test/tradesv3.dryrun.sqlite',
    '/root/telegram-bot/freqtrade_instance_ru/freqtrade_test/user_data/tradesv3.sqlite',
]

for db_path in dbs:
    print(f"\n{'='*60}")
    print(f"DB: {db_path}")
    print('='*60)
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cursor.fetchall()]
        print('Tables:', tables)
        
        if 'trades' in tables:
            cursor.execute('SELECT COUNT(*) FROM trades')
            total = cursor.fetchone()[0]
            print(f'Total trades: {total}')
            
            cursor.execute('SELECT COUNT(*) FROM trades WHERE is_open = 0')
            closed = cursor.fetchone()[0]
            print(f'Closed trades: {closed}')
            
            cursor.execute('SELECT COUNT(*) FROM trades WHERE is_open = 1')
            open_trades = cursor.fetchone()[0]
            print(f'Open trades: {open_trades}')
            
            if closed > 0:
                print(f'\n--- Last 10 CLOSED trades ---')
                cursor.execute('''
                    SELECT id, pair, open_rate, close_rate, open_date, close_date, 
                           close_profit, close_profit_abs, exit_reason, enter_tag, 
                           stake_amount, amount, is_short
                    FROM trades 
                    WHERE is_open = 0 
                    ORDER BY close_date DESC 
                    LIMIT 10
                ''')
                for row in cursor.fetchall():
                    print(row)
            
            if open_trades > 0:
                print(f'\n--- OPEN trades ---')
                cursor.execute('''
                    SELECT id, pair, open_rate, open_date, stake_amount, amount, is_short, enter_tag
                    FROM trades 
                    WHERE is_open = 1 
                    ORDER BY open_date DESC
                ''')
                for row in cursor.fetchall():
                    print(row)
        
        conn.close()
    except Exception as e:
        print(f'Error: {e}')
