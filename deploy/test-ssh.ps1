# SSH check (ASCII only - safe for any PowerShell encoding).
# Run: powershell -ExecutionPolicy Bypass -File .\deploy\test-ssh.ps1
#      powershell -ExecutionPolicy Bypass -File .\deploy\test-ssh.ps1 -ServerHost 1.2.3.4

param(
    [string] $ServerHost = "85.239.53.115",
    [string] $User = "root",
    [string] $KeyPath = "",
    [int] $Port = 22,
    [int] $TcpTimeoutMs = 5000
)

if (-not $KeyPath) {
    $KeyPath = Join-Path $env:USERPROFILE ".ssh\timeweb_key"
}

$sshExe = "C:\Windows\System32\OpenSSH\ssh.exe"
$target = "${User}@${ServerHost}"

Write-Host "=== 1. Key file ===" -ForegroundColor Cyan
if (Test-Path -LiteralPath $KeyPath) {
    Write-Host "OK: $KeyPath"
} else {
    Write-Host "NOT FOUND: $KeyPath" -ForegroundColor Red
    Write-Host "Use .ssh\timeweb_key or pass -KeyPath"
}

Write-Host "`n=== 2. ssh client ===" -ForegroundColor Cyan
if (Test-Path -LiteralPath $sshExe) {
    Write-Host "OpenSSH: $sshExe"
} else {
    Write-Host "MISSING: $sshExe" -ForegroundColor Red
    Write-Host "Install optional feature: OpenSSH Client"
}
$cmd = Get-Command ssh -ErrorAction SilentlyContinue
if ($cmd) {
    Write-Host "ssh resolves to: $($cmd.Source)"
}

Write-Host "`n=== 3. TCP $Port ($ServerHost) max wait ${TcpTimeoutMs}ms ===" -ForegroundColor Cyan
Write-Host "(Test-NetConnection can hang; this uses a short timeout.)"
$tcpOk = $false
$client = $null
try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect($ServerHost, $Port, $null, $null)
    if ($iar.AsyncWaitHandle.WaitOne($TcpTimeoutMs, $false)) {
        try {
            $client.EndConnect($iar)
            $tcpOk = $true
        } catch {
            # ignore
        }
    }
} catch {
    Write-Host "TCP check error: $_" -ForegroundColor Red
} finally {
    if ($null -ne $client) {
        try { $client.Close() } catch { }
    }
}

if ($tcpOk) {
    Write-Host "OK: port $Port is open" -ForegroundColor Green
} else {
    Write-Host "FAIL: no TCP to ${ServerHost}:$Port within $TcpTimeoutMs ms" -ForegroundColor Red
    Write-Host "Check: firewall, antivirus, ISP, VPS panel (SSH on, correct IP)."
}

Write-Host "`n=== 4. Key ACL (UNPROTECTED PRIVATE KEY) ===" -ForegroundColor Cyan
Write-Host "Run PowerShell as Administrator:"
Write-Host "  icacls `"$KeyPath`" /inheritance:r"
Write-Host "  icacls `"$KeyPath`" /grant:r `"$env:USERNAME`:R`""

Write-Host "`n=== 5. Connect ===" -ForegroundColor Cyan
if (Test-Path -LiteralPath $KeyPath) {
    Write-Host "& `"$sshExe`" -p $Port -i `"$KeyPath`" $target"
    Write-Host "`nRun ssh now? (y/N) " -NoNewline
    $a = Read-Host
    if ($a -eq "y" -or $a -eq "Y") {
        & $sshExe -p $Port -i $KeyPath $target
    }
} else {
    Write-Host "Set -KeyPath to an existing key first."
}

Write-Host "`nVerbose:"
Write-Host "  & `"$sshExe`" -v -p $Port -i `"$KeyPath`" $target"
