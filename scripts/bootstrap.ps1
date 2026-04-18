param(
    [switch]$Reinstall
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".venv\\Scripts\\python.exe")) {
    python -m venv .venv
}

$python = Join-Path $root ".venv\\Scripts\\python.exe"
& $python -m pip install --upgrade pip
if ($Reinstall) {
    & $python -m pip install --force-reinstall -e .
} else {
    & $python -m pip install -e .
}

Write-Host "Bootstrap complete. Use scripts\\start-api.ps1 and scripts\\start-loop.ps1"
