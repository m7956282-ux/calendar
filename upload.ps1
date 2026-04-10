# Загрузка main.py на сервер. Запуск: powershell -ExecutionPolicy Bypass -File upload.ps1
$keyPath = Join-Path $env:USERPROFILE ".ssh\timeweb_key"
$server = "root@85.239.53.115"
# Если логин другой — замените root на свой (например ubuntu)
$remotePath = "/opt/telegram-bot/main.py"
$localFile = Join-Path $PSScriptRoot "main.py"

if (-not (Test-Path $localFile)) {
    Write-Host "Файл не найден: $localFile"
    exit 1
}
if (-not (Test-Path $keyPath)) {
    Write-Host "Ключ не найден: $keyPath"
    exit 1
}

$ssh = "C:\Windows\System32\OpenSSH\ssh.exe"
$content = Get-Content -Path $localFile -Raw -Encoding UTF8
$content | & $ssh -i $keyPath $server "cat > $remotePath"
if ($LASTEXITCODE -eq 0) {
    Write-Host "main.py загружен на сервер."
} else {
    Write-Host "Ошибка загрузки. Проверьте ключ и доступ к $server"
}
