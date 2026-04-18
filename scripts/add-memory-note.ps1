param(
    [Parameter(Mandatory=$true)][string]$Title,
    [Parameter(Mandatory=$true)][string]$Content,
    [Parameter(Mandatory=$true)][string]$Wing,
    [Parameter(Mandatory=$true)][string]$Hall,
    [Parameter(Mandatory=$true)][string]$Room,
    [string]$HallType = "hall_discoveries",
    [string]$NoteType = "operator_note",
    [string]$Symbol = "",
    [string]$Session = "",
    [string]$SetupTag = "",
    [string]$StrategyKey = "",
    [double]$Importance = 0.7,
    [string]$Source = "manual",
    [string[]]$Tags = @()
)

$payload = @{
    title = $Title
    content = $Content
    wing = $Wing
    hall = $Hall
    room = $Room
    hall_type = $HallType
    note_type = $NoteType
    symbol = $Symbol
    session = $Session
    setup_tag = $SetupTag
    strategy_key = $StrategyKey
    importance = $Importance
    source = $Source
    tags = $Tags
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Method Post `
    -Uri "http://127.0.0.1:8080/memory/notes" `
    -ContentType "application/json" `
    -Body $payload
