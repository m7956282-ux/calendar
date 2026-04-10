# Почему бот мог «упасть» и как включить автозапуск

## 1. Как понять причину (на сервере)

Выполните по очереди и смотрите вывод.

### Процесс бота
```bash
ps aux | grep '/opt/telegram-bot/main.py'
```

### Хвост лога бота
```bash
tail -100 /opt/telegram-bot/bot.log
```
Ищите: `Traceback`, `ERROR`, `RuntimeError`, `Conflict`, `Unauthorized`, `429`.

### Был ли перезапуск сервера
```bash
last reboot | head -5
uptime
```

### Память (часто убивает процесс OOM)
```bash
dmesg -T | grep -i 'out of memory\|oom\|killed process' | tail -20
free -h
```

### Если уже есть systemd-сервис
```bash
systemctl status maria-telegram-bot.service --no-pager
journalctl -u maria-telegram-bot.service -n 80 --no-pager
```

**Типичные причины:** перезагрузка VPS без автозапуска; ручной `pkill`; нехватка RAM; второй экземпляр с тем же токеном; ошибка в коде после обновления; токен отозван.

---

## 2. Автозапуск через systemd

### Шаг A. Файл с секретами на сервере

```bash
nano /opt/telegram-bot/.env
```

Содержимое (подставьте свои значения), см. `deploy/env.example`:

```env
BOT_TOKEN=ваш_токен_от_BotFather
CHANNEL_ID=-1003727991651
ADMIN_TELEGRAM_ID=8441122963
```

Права:
```bash
chmod 600 /opt/telegram-bot/.env
```

Убедитесь, что в `main.py` токен **не** захардкожен — только переменная окружения (как в текущей версии проекта).

### Шаг B. Установить unit-файл

Скопируйте `deploy/maria-telegram-bot.service` на сервер, например:

```bash
sudo nano /etc/systemd/system/maria-telegram-bot.service
```
(вставьте содержимое файла из репозитория)

### Шаг C. Остановить ручной запуск и включить сервис

```bash
pkill -f '/opt/telegram-bot/main.py' || true
sudo systemctl daemon-reload
sudo systemctl enable maria-telegram-bot.service
sudo systemctl start maria-telegram-bot.service
sudo systemctl status maria-telegram-bot.service
```

### Шаг D. Логи

```bash
tail -f /opt/telegram-bot/bot.log
# или
journalctl -u maria-telegram-bot.service -f
```

После перезагрузки сервис поднимется сам (`Restart=always` перезапустит процесс при падении).

---

## 3. Отключить старый ручной nohup

Не запускайте второй раз `nohup python3 main.py` — будет конфликт `getUpdates`. Достаточно только `systemctl start maria-telegram-bot`.
