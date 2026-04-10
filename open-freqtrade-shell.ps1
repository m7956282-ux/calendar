# PowerShell with Bypass + load .env
# Run: powershell -NoProfile -ExecutionPolicy Bypass -NoExit -File "c:\telegram-bot\open-freqtrade-shell.ps1"

Set-Location $PSScriptRoot

try {
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
} catch { }

. (Join-Path $PSScriptRoot 'load-dotenv.ps1')

$cfg = Join-Path $PSScriptRoot 'freqtrade_test\user_data\config.json'
$ftRoot = Join-Path $PSScriptRoot 'freqtrade_test\freqtrade-develop'

Write-Host ""
Write-Host ".env loaded. Run Freqtrade from this folder (examples):" -ForegroundColor Green
Write-Host ""
Write-Host "  A) If 'freqtrade' is on PATH:" -ForegroundColor Cyan
Write-Host "     freqtrade trade -c `"$cfg`""
Write-Host ""
Write-Host "  B) From this repo's source tree (common here):" -ForegroundColor Cyan
Write-Host "     Set-Location `"$ftRoot`""
Write-Host "     python -m freqtrade trade --config `"$cfg`""
Write-Host ""
Write-Host "  (use the same Python/venv where you installed freqtrade)" -ForegroundColor DarkGray
Write-Host ""
