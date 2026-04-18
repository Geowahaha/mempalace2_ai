$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$logDir = Join-Path $root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$apiLog = Join-Path $logDir "api.log"
$loopLog = Join-Path $logDir "loop.log"

Start-Process powershell `
    -ArgumentList "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "start-api.ps1") `
    -WorkingDirectory $root `
    -RedirectStandardOutput $apiLog `
    -RedirectStandardError $apiLog | Out-Null

Start-Process powershell `
    -ArgumentList "-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", (Join-Path $PSScriptRoot "start-loop.ps1") `
    -WorkingDirectory $root `
    -RedirectStandardOutput $loopLog `
    -RedirectStandardError $loopLog | Out-Null

Write-Host "Started local test server."
Write-Host "API log:  $apiLog"
Write-Host "Loop log: $loopLog"
