param(
    [Parameter(Mandatory = $false, Position = 0)]
    [ValidateSet(
        "help",
        "status",
        "neural",
        "signals",
        "history",
        "performance",
        "positions",
        "scan",
        "scalping_status",
        "scalping_toggle",
        "scalping_logic",
        "pause",
        "resume",
        "close_all"
    )]
    [string]$Action = "status",

    [Parameter(Position = 1)]
    [string]$Task = "",
    [string]$Pair = "",
    [string]$Symbol = "",
    [string]$Enabled = "",
    [string]$Symbols = "",
    [int]$Limit = 30,
    [string]$BaseUrl = "http://127.0.0.1:8788"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$dexterRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Get-BridgeToken {
    $fromEnv = [string]$env:DEXTER_BRIDGE_API_TOKEN
    if ($fromEnv) {
        return $fromEnv.Trim()
    }
    $envFile = Join-Path $dexterRoot ".env.local"
    if (-not (Test-Path $envFile)) {
        return ""
    }
    try {
        $line = Get-Content -Path $envFile -ErrorAction Stop | Where-Object {
            $_ -and ($_ -notmatch '^\s*#') -and ($_ -match '^\s*DEXTER_BRIDGE_API_TOKEN\s*=')
        } | Select-Object -First 1
        if (-not $line) {
            return ""
        }
        $raw = [string]($line -replace '^\s*DEXTER_BRIDGE_API_TOKEN\s*=\s*', '')
        return $raw.Trim().Trim('"').Trim("'")
    }
    catch {
        return ""
    }
}

function Build-Url {
    param(
        [string]$Path,
        [hashtable]$Query = @{}
    )
    $root = ($BaseUrl.TrimEnd("/"))
    $url = "$root$Path"
    if ($Query.Count -gt 0) {
        $parts = @()
        foreach ($k in $Query.Keys) {
            $v = [string]$Query[$k]
            if ($v -ne "") {
                $parts += ("{0}={1}" -f [uri]::EscapeDataString([string]$k), [uri]::EscapeDataString($v))
            }
        }
        if ($parts.Count -gt 0) {
            $url = ("{0}?{1}" -f $url, ($parts -join "&"))
        }
    }
    return $url
}

function Invoke-DexterApi {
    param(
        [string]$Method,
        [string]$Path,
        [hashtable]$Query = @{},
        [object]$Body = $null
    )
    $headers = @{}
    $token = Get-BridgeToken
    if ($token) {
        $headers["Authorization"] = "Bearer $token"
    }
    $url = Build-Url -Path $Path -Query $Query
    if ($null -eq $Body) {
        return Invoke-RestMethod -Method $Method -Uri $url -Headers $headers -TimeoutSec 30
    }
    return Invoke-RestMethod -Method $Method -Uri $url -Headers $headers -TimeoutSec 30 -Body ($Body | ConvertTo-Json -Depth 8) -ContentType "application/json"
}

try {
    $result = $null
    $symbolTaskMap = @{
        "XAU" = "XAUUSD"
        "XAUUSD" = "XAUUSD"
        "GOLD" = "XAUUSD"
        "ETH" = "ETHUSD"
        "ETHUSD" = "ETHUSD"
        "BTC" = "BTCUSD"
        "BTCUSD" = "BTCUSD"
        "GBP" = "GBPUSD"
        "GBPUSD" = "GBPUSD"
    }
    switch ($Action) {
        "help" {
            $result = @{
                actions = @(
                    "status", "neural", "signals", "history", "performance", "positions",
                    "scan", "scalping_status", "scalping_toggle", "scalping_logic",
                    "pause", "resume", "close_all"
                )
                examples = @(
                    "dexter_ops.ps1 status",
                    "dexter_ops.ps1 scan -Task xauusd",
                    "dexter_ops.ps1 scalping_logic -Symbol BTCUSD"
                )
            }
        }
        "status" {
            $result = Invoke-DexterApi -Method "GET" -Path "/api/status"
        }
        "neural" {
            $result = Invoke-DexterApi -Method "GET" -Path "/api/neural/status"
        }
        "signals" {
            $result = Invoke-DexterApi -Method "GET" -Path "/api/signals/active"
        }
        "history" {
            $q = @{}
            if ($Symbol) { $q["symbol"] = $Symbol }
            $q["limit"] = [string]([Math]::Max(1, [Math]::Min(200, $Limit)))
            $result = Invoke-DexterApi -Method "GET" -Path "/api/signals/history" -Query $q
        }
        "performance" {
            $result = Invoke-DexterApi -Method "GET" -Path "/api/performance"
        }
        "positions" {
            $result = Invoke-DexterApi -Method "GET" -Path "/api/positions"
        }
        "scan" {
            $scanTask = [string]$Task
            if (-not $scanTask -and $Pair) { $scanTask = [string]$Pair }
            if (-not $scanTask) { $scanTask = "scalping" }

            $scanNorm = $scanTask.Trim().ToUpperInvariant()
            if ($symbolTaskMap.ContainsKey($scanNorm)) {
                $sym = $symbolTaskMap[$scanNorm]
                $result = Invoke-DexterApi -Method "GET" -Path "/api/scalping/logic" -Query @{ symbol = $sym }
            }
            else {
                $result = Invoke-DexterApi -Method "POST" -Path "/api/scan/run" -Query @{ task = $scanTask }
            }
        }
        "scalping_status" {
            $result = Invoke-DexterApi -Method "GET" -Path "/api/scalping/status"
        }
        "scalping_toggle" {
            if (-not $Enabled) {
                throw "Enabled is required for scalping_toggle (use 1 or 0)."
            }
            $q = @{ enabled = $Enabled }
            if ($Symbols) { $q["symbols"] = $Symbols }
            $result = Invoke-DexterApi -Method "POST" -Path "/api/scalping/toggle" -Query $q
        }
        "scalping_logic" {
            $sym = [string]$Symbol
            if (-not $sym) { $sym = "BTCUSD" }
            $result = Invoke-DexterApi -Method "GET" -Path "/api/scalping/logic" -Query @{ symbol = $sym }
        }
        "pause" {
            $result = Invoke-DexterApi -Method "POST" -Path "/api/action/pause"
        }
        "resume" {
            $result = Invoke-DexterApi -Method "POST" -Path "/api/action/resume"
        }
        "close_all" {
            $result = Invoke-DexterApi -Method "POST" -Path "/api/action/close_all"
        }
        default {
            throw "Unsupported action: $Action"
        }
    }

    [pscustomobject]@{
        ok = $true
        action = $Action
        result = $result
    } | ConvertTo-Json -Depth 10
}
catch {
    [pscustomobject]@{
        ok = $false
        action = $Action
        error = $_.Exception.Message
    } | ConvertTo-Json -Depth 8
    exit 1
}
