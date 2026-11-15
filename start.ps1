# AccessSentinel - one-click start for Windows (run start.bat, or: powershell -ExecutionPolicy Bypass -File start.ps1)
#
# First run: creates .env from .env.example with random passwords, builds and starts everything in Docker.
# Later runs: just starts it again. Your data is kept between runs (stop.bat stops it).
$ErrorActionPreference = "Continue"
Set-Location -LiteralPath $PSScriptRoot

function Fail([string]$msg) {
    Write-Host ""
    Write-Host $msg -ForegroundColor Red
    if (-not $env:AS_NONINTERACTIVE) { Read-Host "Press Enter to close" | Out-Null }
    exit 1
}

Write-Host "AccessSentinel - starting" -ForegroundColor Cyan

# 1. Docker must be installed and running
cmd /c "docker info >nul 2>&1"
if ($LASTEXITCODE -ne 0) {
    Fail ("Docker is not running.`n" +
          "  1. Install Docker Desktop: https://www.docker.com/products/docker-desktop/`n" +
          "  2. Start Docker Desktop and wait until it says 'Engine running'.`n" +
          "  3. Run start.bat again.")
}
$mem = (cmd /c "docker info --format {{.MemTotal}} 2>nul") -as [double]
if ($mem -and $mem -lt 6GB) {
    Write-Host ("Warning: Docker has only {0:N1} GB of memory; about 8 GB is recommended " -f ($mem / 1GB)) -ForegroundColor Yellow
    Write-Host "(Docker Desktop > Settings > Resources, or .wslconfig - see docs/SETUP_WINDOWS.md)." -ForegroundColor Yellow
}

# 2. Settings file with random secrets (first run only)
if (-not (Test-Path -LiteralPath ".env")) {
    if (-not (Test-Path -LiteralPath ".env.example")) { Fail ".env.example is missing - the folder is incomplete." }
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $lines = foreach ($line in [IO.File]::ReadAllLines((Join-Path $PSScriptRoot ".env.example"))) {
        if ($line -match '^([A-Z0-9_]+)=change-me\s*$') {
            $bytes = New-Object byte[] 18
            $rng.GetBytes($bytes)
            $secret = [Convert]::ToBase64String($bytes).Replace('+', 'x').Replace('/', 'y').Replace('=', '')
            "$($Matches[1])=$secret"
        } else { $line }
    }
    # UTF-8 without BOM, LF line endings (Docker Compose reads it)
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot ".env"), (($lines -join "`n") + "`n"), (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "Created .env with new random passwords." -ForegroundColor Green
} elseif (Select-String -LiteralPath ".env" -Pattern "=change-me" -Quiet) {
    Fail (".env still contains 'change-me' values.`n" +
          "Delete .env and run start.bat again (it creates one with random passwords).`n" +
          "If you already started AccessSentinel with that .env, first run:  docker compose down -v")
}

# 3. Build and start (first time: 10-20 minutes, downloads about 3 GB)
Write-Host "Building and starting the containers (first time 10-20 minutes)..."
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { Fail "docker compose up failed - see the messages above." }

# 4. Wait until every service reports healthy
Write-Host "Waiting for all services to become healthy" -NoNewline
$deadline = (Get-Date).AddMinutes(10)
$ready = $false
while ((Get-Date) -lt $deadline) {
    $status = @(cmd /c "docker compose ps --format ""{{.Service}} {{.Status}}"" 2>nul")
    $notReady = @($status | Where-Object { $_ -and ($_ -notmatch '\(healthy\)') })
    if ($status.Count -gt 0 -and $notReady.Count -eq 0) { $ready = $true; break }
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 10
}
Write-Host ""
if (-not $ready) {
    docker compose ps
    Fail "Some services are not healthy after 10 minutes (see the list above). Try: docker compose logs <service>"
}

Write-Host ""
Write-Host "AccessSentinel is running." -ForegroundColor Green
Write-Host "  Dashboard (security team):  http://localhost:8080   users: secadmin, analyst1"
Write-Host "  Protected apps (employees): http://localhost:8000   users: alice, bob, carol, ... "
Write-Host "  Passwords: open the .env file in this folder:"
Write-Host "     AS_ANALYST_PASSWORD   -> secadmin / analyst1"
Write-Host "     AS_DEMO_USER_PASSWORD -> alice, bob, carol, ..."
Write-Host "  Demo script: PRESENTATION_DEMO.md      Stop: stop.bat"
if (-not $env:AS_NONINTERACTIVE) {
    Start-Process "http://localhost:8080"
    Read-Host "Press Enter to close this window (AccessSentinel keeps running)" | Out-Null
}
