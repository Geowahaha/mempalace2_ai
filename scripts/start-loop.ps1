param(
    [double]$Interval,
    [switch]$NoDryRun
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtualenv missing. Run scripts\\bootstrap.ps1 first."
}

$args = @("-m", "trading_ai")
if ($PSBoundParameters.ContainsKey("Interval")) {
    $args += @("--interval", "$Interval")
}
if ($NoDryRun) {
    $args += "--no-dry-run"
} else {
    $args += "--dry-run"
}

& $python @args
