param(
    [switch]$KeepApi
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$selfPid = $PID

$targets = Get-CimInstance Win32_Process |
    Where-Object {
        $cmd = [string]$_.CommandLine
        if (-not $cmd -or $_.ProcessId -eq $selfPid) {
            return $false
        }
        if ($cmd -notlike "*$root*") {
            return $false
        }
        if ($cmd -like "*ollama*") {
            return $false
        }
        if ($KeepApi -and ($cmd -like "*trading_ai.api*" -or $cmd -like "*start-api.ps1*")) {
            return $false
        }
        return (
            $cmd -like "* -m trading_ai --*" -or
            $cmd -like "*trading_ai.api*" -or
            $cmd -like "*run-demo-live-loop.ps1*" -or
            $cmd -like "*start-api.ps1*"
        )
    } |
    Sort-Object ProcessId -Descending

if (-not $targets) {
    Write-Host "No Mempalac API/loop processes found."
    exit 0
}

foreach ($target in $targets) {
    Write-Host "Stopping PID $($target.ProcessId): $($target.Name)"
    Stop-Process -Id $target.ProcessId -Force -ErrorAction SilentlyContinue
}

Write-Host "Stopped Mempalac demo-live stack. Ollama and Dexter were not touched."
