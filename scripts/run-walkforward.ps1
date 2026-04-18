$ErrorActionPreference = "Stop"

$repo = Split-Path $PSScriptRoot -Parent
Set-Location $repo

$defaults = @(
    "--start", "2026-03-16",
    "--end", "2026-04-11",
    "--timezone", "Asia/Bangkok",
    "--source-policy", "real_only",
    "--enable-learning"
)

if ($args.Count -gt 0) {
    & "$repo\.venv\Scripts\python.exe" -m trading_ai.backtest @args
} else {
    & "$repo\.venv\Scripts\python.exe" -m trading_ai.backtest @defaults
}
