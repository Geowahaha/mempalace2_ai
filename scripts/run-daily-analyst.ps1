param(
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtualenv missing. Run scripts\\bootstrap.ps1 first."
}

$args = @("-m", "trading_ai.daily_analyst")
if ($DryRun) {
    $args += "--dry-run"
}

& $python @args
