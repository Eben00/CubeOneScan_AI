# Run on the SAME PC that runs the connector.
# Usage: .\diagnose-connection.ps1
# Optional: .\diagnose-connection.ps1 -LanIp 192.168.8.105

param([string]$LanIp = "192.168.8.105")

Write-Host "=== CubeOneScan connector connectivity ===" -ForegroundColor Cyan
Write-Host ""

Write-Host "Step 1 - Is anything listening on TCP 8080?"
$listen = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue
if ($listen) {
    $listen | Format-Table LocalAddress, LocalPort, OwningProcess -AutoSize
} else {
    Write-Host "  NOTHING on 8080. Start: cd $PSScriptRoot ; npm start" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 2 - Localhost healthz"
try {
    $r = Invoke-RestMethod -Uri "http://127.0.0.1:8080/healthz" -TimeoutSec 5
    Write-Host "  OK:" $r
} catch {
    Write-Host "  FAIL:" $_.Exception.Message -ForegroundColor Red
}

Write-Host ""
Write-Host "Step 3 - LAN IP healthz (must work from this PC for phone to work)"
Write-Host "  URL: http://${LanIp}:8080/healthz"
try {
    $r2 = Invoke-RestMethod -Uri "http://${LanIp}:8080/healthz" -TimeoutSec 5
    Write-Host "  OK:" $r2
} catch {
    Write-Host "  FAIL:" $_.Exception.Message -ForegroundColor Red
    Write-Host "  Tip: Wi-Fi Private profile, firewall TCP 8080, server must listen on 0.0.0.0" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Step 4 - Quick port test"
$tnc = Test-NetConnection -ComputerName $LanIp -Port 8080 -WarningAction SilentlyContinue
Write-Host "  TcpTestSucceeded:" $tnc.TcpTestSucceeded " PingSucceeded:" $tnc.PingSucceeded

Write-Host ""
Write-Host "Firewall (run PowerShell as Administrator):" -ForegroundColor Cyan
Write-Host '  netsh advfirewall firewall add rule name="CubeOneScan Connector 8080" dir=in action=allow protocol=TCP localport=8080'
