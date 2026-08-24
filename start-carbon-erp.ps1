param(
    [switch]$NoBackend
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$BackendDir = Join-Path $ScriptDir "resources\app\backend"
$ElectronExe = Join-Path $ScriptDir "Carbon ERP.exe"

if (-not (Test-Path $ElectronExe)) {
    Write-Error "Electron executable not found at: $ElectronExe"
    exit 1
}

if (-not $NoBackend) {
    Write-Host "Starting backend server on http://localhost:3001..." -ForegroundColor Green
    $backendProcess = Start-Process -FilePath "node" -ArgumentList "server.js" -WorkingDirectory $BackendDir -PassThru -WindowStyle Hidden

    Write-Host "Waiting for backend to start..." -ForegroundColor Yellow
    Start-Sleep -Seconds 3

    try {
        $health = Invoke-RestMethod -Uri "http://localhost:3001/health" -Method Get -TimeoutSec 5
        Write-Host "Backend health check: $($health.status)" -ForegroundColor Green
    } catch {
        Write-Warning "Backend health check failed: $_"
    }
}

Write-Host "Starting Carbon ERP application..." -ForegroundColor Green
$electronProcess = Start-Process -FilePath $ElectronExe -PassThru

Write-Host ""
Write-Host "Both processes started successfully!" -ForegroundColor Cyan
Write-Host "Backend: http://localhost:3001" -ForegroundColor Gray
Write-Host "Dashboard: http://localhost:3001/dashboard.html" -ForegroundColor Gray
Write-Host ""
Write-Host "Press any key to stop both processes..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")

Write-Host "Stopping processes..." -ForegroundColor Yellow
if ($backendProcess) {
    Stop-Process -Id $backendProcess.Id -Force -ErrorAction SilentlyContinue
}
Stop-Process -Id $electronProcess.Id -Force -ErrorAction SilentlyContinue
Write-Host "Done." -ForegroundColor Green