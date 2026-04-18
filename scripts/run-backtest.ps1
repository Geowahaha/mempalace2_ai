$ErrorActionPreference = "Stop"

$repo = Split-Path $PSScriptRoot -Parent
Set-Location $repo

& "$repo\.venv\Scripts\python.exe" -m trading_ai.backtest @args
