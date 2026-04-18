param(
    [string]$BridgeRoot = "D:\dexter_pro_v3_fixed\_refs\OpenClaw-MT5-python-bridge",
    [string]$PythonExe = "C:\Python312\python.exe",
    [int]$Port = 18812,
    [int]$IntervalSec = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$loopScript = Join-Path $PSScriptRoot "mt5_bridge_guard_loop.ps1"
if (-not (Test-Path $loopScript)) {
    throw "Loop script missing: $loopScript"
}

$startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"
if (-not (Test-Path $startupDir)) {
    New-Item -Path $startupDir -ItemType Directory -Force | Out-Null
}

$cmdPath = Join-Path $startupDir "Dexter_MT5_Bridge_Watchdog.cmd"
$cmd = @"
@echo off
powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "$loopScript" -BridgeRoot "$BridgeRoot" -PythonExe "$PythonExe" -Port $Port -IntervalSec $IntervalSec
"@
Set-Content -Path $cmdPath -Value $cmd -Encoding ASCII

# Prevent duplicate loop instances.
$loopSig = "mt5_bridge_guard_loop.ps1"
$existing = @(
    Get-CimInstance Win32_Process -Filter "name='powershell.exe'" |
        Where-Object { [string]($_.CommandLine) -like "*$loopSig*" }
)
if ($existing.Count -eq 0) {
    Start-Process -FilePath powershell -ArgumentList @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-WindowStyle", "Hidden",
        "-File", $loopScript,
        "-BridgeRoot", $BridgeRoot,
        "-PythonExe", $PythonExe,
        "-Port", "$Port",
        "-IntervalSec", "$IntervalSec"
    ) -WindowStyle Hidden
}

Write-Host "Installed startup watchdog:"
Write-Host "  $cmdPath"

Write-Host ""
Write-Host "Running watchdog processes:"
Get-CimInstance Win32_Process -Filter "name='powershell.exe'" |
    Where-Object { [string]($_.CommandLine) -like "*$loopSig*" } |
    Select-Object ProcessId, CommandLine |
    Format-Table -AutoSize
