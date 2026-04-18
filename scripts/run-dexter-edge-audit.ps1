param(
    [string]$DexterRoot = "D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed",
    [string]$Output = "D:\Mempalac_AI\reports\DEXTER_EDGE_AUDIT.md",
    [string]$TradeExport = "",
    [switch]$NoEnv,
    [int]$MaxEnvKeys = 260
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$python = Join-Path $root ".venv\\Scripts\\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtualenv missing. Run scripts\\bootstrap.ps1 first."
}

$args = @(
    "-m", "trading_ai.dexter_edge_audit",
    "--dexter-root", $DexterRoot,
    "--output", $Output,
    "--max-env-keys", "$MaxEnvKeys"
)

if ($TradeExport) {
    $args += @("--trade-export", $TradeExport)
}

if ($NoEnv) {
    $args += "--no-env"
}

& $python @args
