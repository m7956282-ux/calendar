@echo off
cd /d "%~dp0"
rem Запуск без "start" — одно окно PowerShell с Bypass (можно также вызывать .ps1 напрямую из каталога)
powershell -NoProfile -ExecutionPolicy Bypass -NoExit -File "%~dp0open-freqtrade-shell.ps1"
