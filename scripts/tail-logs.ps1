param(
    [int]$Lines = 80
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root "logs"

$apiLog = Join-Path $logDir "api.out.log"
$apiErr = Join-Path $logDir "api.err.log"
$loopLog = Join-Path $logDir "demo-live-loop.out.log"
$loopErr = Join-Path $logDir "demo-live-loop.err.log"

Write-Host "=== API OUT ==="
if (Test-Path $apiLog) {
    Get-Content $apiLog -Tail $Lines
} else {
    Write-Host "missing: $apiLog"
}

Write-Host ""
Write-Host "=== API ERR ==="
if (Test-Path $apiErr) {
    Get-Content $apiErr -Tail $Lines
} else {
    Write-Host "missing: $apiErr"
}

Write-Host ""
Write-Host "=== LOOP OUT ==="
if (Test-Path $loopLog) {
    Get-Content $loopLog -Tail $Lines
} else {
    Write-Host "missing: $loopLog"
}

Write-Host ""
Write-Host "=== LOOP ERR ==="
if (Test-Path $loopErr) {
    Get-Content $loopErr -Tail $Lines
} else {
    Write-Host "missing: $loopErr"
}
