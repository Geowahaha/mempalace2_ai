param(
    [string]$BridgeRoot = "D:\dexter_pro_v3_fixed\_refs\OpenClaw-MT5-python-bridge",
    [string]$PythonExe = "C:\Python312\python.exe",
    [int]$Port = 18812,
    [int]$IntervalSec = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$watchdogScript = Join-Path $PSScriptRoot "mt5_bridge_watchdog.ps1"
if (-not (Test-Path $watchdogScript)) {
    throw "watchdog script not found: $watchdogScript"
}

while ($true) {
    try {
        & $watchdogScript -BridgeRoot $BridgeRoot -PythonExe $PythonExe -Port $Port | Out-Null
    } catch {
        # keep loop alive; error details are already persisted by watchdog log.
    }
    Start-Sleep -Seconds ([Math]::Max(5, [int]$IntervalSec))
}

