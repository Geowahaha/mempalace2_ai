param(
    [string]$OutputPath = ""
)

$data = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8080/memory/analyst-packet"
if ($OutputPath) {
    $dir = Split-Path -Parent $OutputPath
    if ($dir) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    $data | ConvertTo-Json -Depth 8 | Set-Content -Path $OutputPath -Encoding UTF8
    Write-Host "Saved analyst packet to $OutputPath"
} else {
    $data | ConvertTo-Json -Depth 8
}
