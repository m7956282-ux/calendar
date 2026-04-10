# Что где делать: сервер и ПК

## Важно

- **На сервере** ставят Python, venv и зависимости. Ничего не «качают с ПК».
- **С ПК** на сервер нужно только **передать файлы** `main.py` и `requirements.txt`.

Если загрузка с ПК не срабатывала (команда обрезалась, большой вставки не было) — на сервере мог не оказаться `main.py`. Ниже — как всё проверить и доделать.

---

## 1. Подключиться к серверу

Через веб-консоль Timeweb или SSH, например:

```bash
ssh -i путь/к/ключу root@85.239.53.115
```

(или ваш логин вместо `root`)

---

## 2. На СЕРВЕРЕ — проверить и установить Python

Выполните по очереди и смотрите вывод.

```bash
python3 --version
```

- Если версия есть (например 3.10 или 3.12) — Python уже стоит.
- Если «command not found» — установите:

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv
```

Потом снова:

```bash
python3 --version
pip3 --version
```

---

## 3. На СЕРВЕРЕ — каталог и venv

```bash
mkdir -p /opt/telegram-bot
cd /opt/telegram-bot
python3 -m venv venv
source venv/bin/activate
```

После этого в начале строки должно быть `(venv)`.

---

## 4. На СЕРВЕРЕ — requirements.txt

Проверка:

```bash
cat /opt/telegram-bot/requirements.txt
```

Должны быть строки вроде:

- `python-telegram-bot>=21.0,<22.0`
- `openai>=1.0.0`

Если файла нет или он пустой/неправильный, создайте:

```bash
cd /opt/telegram-bot
cat > requirements.txt << 'END'
python-telegram-bot>=21.0,<22.0
openai>=1.0.0
END
```

---

## 5. На СЕРВЕРЕ — есть ли main.py

```bash
ls -la /opt/telegram-bot/main.py
head -5 /opt/telegram-bot/main.py
```

- Если `main.py` есть и в начале видно `import logging` и т.п. — файл с ПК уже загружен, переходите к шагу 7.
- Если файла нет или он пустой/битый — его нужно снова загрузить **с ПК** (шаг 6).

---

## 6. С ПК — загрузить main.py на сервер

На вашем ПК в PowerShell (из папки с ботом):

```powershell
cd C:\telegram-bot
powershell -ExecutionPolicy Bypass -File upload.ps1
```

Если в скрипте указан не ваш логин — откройте `upload.ps1` и замените `root` в адресе сервера на свой логин.

После успешного запуска снова на сервере выполните проверку из шага 5.

---

## 7. На СЕРВЕРЕ — зависимости и запуск бота

```bash
cd /opt/telegram-bot
source venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

Если всё ок, в логе будет что-то вроде «Бот запущен. Ожидаю сообщения...».

---

## Кратко

| Где      | Что делаем |
|----------|------------|
| Сервер   | Установить Python (apt), создать venv, создать/проверить requirements.txt |
| ПК       | Загрузить на сервер только файлы: main.py (и при необходимости requirements.txt) |
| Сервер   | pip install -r requirements.txt и python3 main.py |

Если пришлите вывод команд из шагов 2, 5 и 7 (или скрин), можно точечно сказать, что не так и что выполнить дальше.

---

## VK Rent Bot (Timeweb) — что делать после изменений в `vk_rent_bot.py`

### Вариант А: через SFTP (сессия уже подключена или подключаешься заново)

**На ПК.** Открыть sftp (с ключом):

```
sftp -i $env:USERPROFILE\.ssh\timeweb_key root@85.239.53.115
```

Внутри `sftp>` выполнить по очереди:

```
put "C:\telegram-bot\vk_rent_bot.py" /tmp/vk_rent_bot.py
```

```
ls -l /tmp/vk_rent_bot.py
```

```
exit
```

Если `put` выдаст ошибку — пришли точный текст. С путём в кавычках обычно срабатывает.

### Вариант Б: через SCP (одной командой с ПК)

**На ПК.** В PowerShell:

```
scp -i $env:USERPROFILE\.ssh\timeweb_key C:\telegram-bot\vk_rent_bot.py root@85.239.53.115:/tmp/vk_rent_bot.py
```

### Как попасть на сервер (SSH)

**На ПК.** В PowerShell:

```
ssh -i $env:USERPROFILE\.ssh\timeweb_key root@85.239.53.115
```

(Если без ключа: `ssh root@85.239.53.115` — тогда запросит пароль.)

### Если из PowerShell не заходит на сервер (Windows)

**Быстрая проверка одним скриптом** (из папки `c:\telegram-bot`):

```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\test-ssh.ps1
```

Скрипт на **английском (ASCII)** — чтобы PowerShell 5.1 не ломал парсер из‑за UTF‑8 без BOM. Порт 22 проверяется через `TcpClient` с **таймаутом ~5 с** (в отличие от `Test-NetConnection`, который часто долго висит на «Waiting for response»).

1. **Убедиться, что вызывается встроенный OpenSSH**, а не другой `ssh` (Git и т.п.):

   ```powershell
   Get-Command ssh | Format-List *
   ```

   При необходимости вызывать явно:

   ```powershell
   & "C:\Windows\System32\OpenSSH\ssh.exe" -i "$env:USERPROFILE\.ssh\timeweb_key" root@85.239.53.115
   ```

2. **Ошибка про незащищённый ключ** (`UNPROTECTED PRIVATE KEY FILE!`) — Windows требует жёсткие права на файл ключа. В PowerShell **от администратора**:

   ```powershell
   icacls "$env:USERPROFILE\.ssh\timeweb_key" /inheritance:r
   icacls "$env:USERPROFILE\.ssh\timeweb_key" /grant:r "$env:USERNAME`:R"
   ```

   (Если ключ называется иначе — подставьте свой путь.)

3. **Порт 22 до сервера** (обрыв/таймаут — чаще сеть или фаервол, не PowerShell):

   ```powershell
   Test-NetConnection -ComputerName 85.239.53.115 -Port 22
   ```

   `TcpTestSucceeded : False` — с ПК до SSH не достучаться (провайдер, антивирус, фаервол Windows, на стороне хостинга отключён SSH — смотреть панель Timeweb).

4. **Подробный лог**, чтобы увидеть, на чём падает:

   ```powershell
   ssh -v -i "$env:USERPROFILE\.ssh\timeweb_key" root@85.239.53.115
   ```

5. **`Host key verification failed`** — ключ сервера сменился. Сбросить запись (осторожно: убедитесь, что это ваш сервер):

   ```powershell
   ssh-keygen -R 85.239.53.115
   ```

6. **`Permission denied (publickey)`** — не тот ключ, не тот пользователь или на сервере не добавлен ваш публичный ключ. В панели VPS проверьте привязку SSH-ключа к серверу.

Пришлите **точный текст ошибки** из PowerShell (или последние 15–20 строк `ssh -v`) — по ним можно сказать точнее.

### Дальше — на сервере (SSH)

**На ПК** (если ещё не в сессии) — подключиться:

```
ssh -i $env:USERPROFILE\.ssh\timeweb_key root@85.239.53.115
```

**На сервере** (в открывшейся SSH-сессии). Переложить файл и перезапустить бота:

```
sudo mv /tmp/vk_rent_bot.py /opt/telegram-bot-vk/vk_rent_bot.py
```

```
sudo systemctl restart vk-rent-bot
```

**На сервере.** Проверить, что бот поднялся и в логах новая версия:

```
sudo systemctl status vk-rent-bot
```

```
journalctl -u vk-rent-bot --since "2 minutes ago" --no-pager -l
```

В статусе должно быть `active (running)`. Выход из просмотра: клавиша `q`.

## VK Calendar mirror (свой календарь занятости)

Файл сервиса: `C:\telegram-bot\vk_calendar_server.py`.
Он читает брони из `rent_bot.db` и показывает календарь на веб-странице.

Открыть вручную:
- `http://SERVER_IP:8090/calendar` — страница календаря
- `http://SERVER_IP:8090/api/slots?days=14` — JSON API
- `http://SERVER_IP:8090/health` — health-check

### Разовый запуск для проверки

**На сервере (после ssh):**

```bash
python3 /opt/telegram-bot-vk/vk_calendar_server.py
```

Остановить: `Ctrl+C`.

### Установка как systemd-сервис

**На сервере (после ssh):**

```bash
sudo tee /etc/systemd/system/vk-rent-calendar.service > /dev/null <<'EOF'
[Unit]
Description=VK rent calendar mirror
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/telegram-bot-vk
ExecStart=/opt/telegram-bot/venv/bin/python /opt/telegram-bot-vk/vk_calendar_server.py
Restart=always
RestartSec=3
Environment=VK_CALENDAR_HOST=0.0.0.0
Environment=VK_CALENDAR_PORT=8090
Environment=VK_CALENDAR_DEFAULT_DAYS=14
Environment=VK_RENT_DB_PATH=/opt/telegram-bot/rent_bot.db

[Install]
WantedBy=multi-user.target
EOF
```

**На сервере (после ssh):**

```bash
sudo systemctl daemon-reload
```

**На сервере (после ssh):**

```bash
sudo systemctl enable --now vk-rent-calendar
```

**На сервере (после ssh):**

```bash
sudo systemctl status vk-rent-calendar
```

**На сервере (после ssh):**

```bash
journalctl -u vk-rent-calendar --since "2 minutes ago" --no-pager -l
```

### GitHub Pages: независимый календарь из `data/bookings.json`

Идея: бот обновляет `data/bookings.json` через GitHub API при каждом изменении брони.  
Страница `index.html` в репозитории читает этот JSON напрямую.

**На сервере (после ssh) — добавить переменные в сервис VK бота (`/etc/systemd/system/vk-rent-bot.service`):**

```bash
sudo nano /etc/systemd/system/vk-rent-bot.service
```

Добавьте в блок `[Service]`:

```text
Environment=CALENDAR_GH_OWNER=m7956282-ux
Environment=CALENDAR_GH_REPO=calendar
Environment=CALENDAR_GH_BRANCH=main
Environment=CALENDAR_GH_PATH=data/bookings.json
Environment=CALENDAR_GH_TOKEN=github_pat_xxx
Environment=CALENDAR_SYNC_PAST_DAYS=30
Environment=CALENDAR_SYNC_FUTURE_DAYS=180
Environment=CALENDAR_SYNC_TIMEOUT_SECONDS=10
```

**На сервере (после ssh) — применить и перезапустить:**

```bash
sudo systemctl daemon-reload
```

```bash
sudo systemctl restart vk-rent-bot
```

**На сервере (после ssh) — проверить логи синка:**

```bash
journalctl -u vk-rent-bot --since "10 minutes ago" --no-pager -l | grep -E "GitHub calendar sync"
```

Ожидается строка `GitHub calendar sync ok ...`.
