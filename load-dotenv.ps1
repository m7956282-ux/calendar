# Load key=value pairs from .env into current PowerShell session (UTF-8).
# If execution policy blocks: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
# Then: . .\load-dotenv.ps1

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$envFile = Join-Path $root '.env'
if (-not (Test-Path $envFile)) {
    Write-Error "Missing file: $envFile"
    exit 1
}
Get-Content $envFile -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith('#')) { return }
    $eq = $line.IndexOf('=')
    if ($eq -lt 1) { return }
    $name = $line.Substring(0, $eq).Trim()
    $val = $line.Substring($eq + 1).Trim()
    if (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'"))) {
        $val = $val.Substring(1, $val.Length - 2)
    }
    Set-Item -Path "Env:$name" -Value $val
}
Write-Host "OK: loaded $envFile"
