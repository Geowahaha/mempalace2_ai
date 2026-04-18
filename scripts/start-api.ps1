param(
    [int]$Port
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtualenv missing. Run scripts\\bootstrap.ps1 first."
}

if ($PSBoundParameters.ContainsKey("Port")) {
    $env:API_PORT = "$Port"
}

& $python -m trading_ai.api
