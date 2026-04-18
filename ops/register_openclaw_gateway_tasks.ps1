param(
    [string]$TaskPrefix = "Dexter-OpenClaw-Gateway",
    [int]$Port = 18789,
    [string]$GatewayCmd = "C:\Users\mrgeo\.openclaw\gateway.cmd",
    [switch]$RunAsSystem,
    [switch]$DisableLegacyTask = $true
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$defaultPort = 18789
$defaultGatewayCmd = "C:\Users\mrgeo\.openclaw\gateway.cmd"

$watchdogScript = Join-Path $PSScriptRoot "openclaw_gateway_watchdog.ps1"
$taskShimDir = Join-Path $env:LOCALAPPDATA "DexterTaskShims"
$taskShim = Join-Path $taskShimDir "openclaw_gateway_watchdog.vbs"
if (-not (Test-Path $watchdogScript)) {
    throw "Watchdog script not found: $watchdogScript"
}
if (-not (Test-Path $GatewayCmd)) {
    throw "Gateway launcher not found: $GatewayCmd"
}

$psExe = "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe"
$wscriptExe = "$env:WINDIR\System32\wscript.exe"
$psArgs = @(
    "-NonInteractive",
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $watchdogScript
)
if ($Port -ne $defaultPort) {
    $psArgs += @("-Port", "$Port")
}
if ($GatewayCmd -ne $defaultGatewayCmd) {
    $psArgs += @("-GatewayCmd", $GatewayCmd)
}
$psCmd = ((@($psExe) + $psArgs) | ForEach-Object {
    '"' + (($_ -as [string]) -replace '"', '""') + '"'
}) -join " "
if (-not (Test-Path $taskShimDir)) {
    New-Item -Path $taskShimDir -ItemType Directory -Force | Out-Null
}
$vbsBody = @"
Option Explicit
Dim sh
Set sh = CreateObject("WScript.Shell")
sh.Run "$($psCmd -replace '"', '""')", 0, False
"@
Set-Content -Path $taskShim -Value $vbsBody -Encoding ASCII
$arg = "//B //nologo `"$taskShim`""
$action = New-ScheduledTaskAction -Execute $wscriptExe -Argument $arg

if ($RunAsSystem) {
    $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
} else {
    $principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
}

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5)

$autostartName = "$TaskPrefix-Autostart"
$healthName = "$TaskPrefix-Health"

try { Unregister-ScheduledTask -TaskName $autostartName -Confirm:$false -ErrorAction SilentlyContinue } catch {}
try { Unregister-ScheduledTask -TaskName $healthName -Confirm:$false -ErrorAction SilentlyContinue } catch {}
try { schtasks /Delete /F /TN $autostartName *> $null } catch {}
try { schtasks /Delete /F /TN $healthName *> $null } catch {}

$triggers = @()
if ($RunAsSystem) {
    $triggers += New-ScheduledTaskTrigger -AtStartup
    $triggers += New-ScheduledTaskTrigger -AtLogOn
} else {
    $triggers += New-ScheduledTaskTrigger -AtLogOn
}

$taskAutostart = New-ScheduledTask -Action $action -Trigger $triggers -Principal $principal -Settings $settings
try {
    Register-ScheduledTask -TaskName $autostartName -InputObject $taskAutostart -Force | Out-Null
} catch {
    $taskCmd = "`"$wscriptExe`" //B //nologo `"$taskShim`""
    if ($RunAsSystem) {
        schtasks /Create /F /TN $autostartName /RU SYSTEM /SC ONSTART /TR $taskCmd *> $null
    } else {
        $ru = "$env:USERNAME"
        schtasks /Create /F /TN $autostartName /RU $ru /SC ONLOGON /RL LIMITED /TR $taskCmd *> $null
    }
}

$triggerRepeat = New-ScheduledTaskTrigger `
    -Once -At (Get-Date).Date.AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 1) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$taskHealth = New-ScheduledTask -Action $action -Trigger $triggerRepeat -Principal $principal -Settings $settings
try {
    Register-ScheduledTask -TaskName $healthName -InputObject $taskHealth -Force | Out-Null
} catch {
    $taskCmd = "`"$wscriptExe`" //B //nologo `"$taskShim`""
    if ($RunAsSystem) {
        schtasks /Create /F /TN $healthName /RU SYSTEM /SC MINUTE /MO 1 /TR $taskCmd *> $null
    } else {
        $ru = "$env:USERNAME"
        schtasks /Create /F /TN $healthName /RU $ru /SC MINUTE /MO 1 /TR $taskCmd *> $null
    }
}

if ($DisableLegacyTask -and (Get-ScheduledTask -TaskName "OpenClaw Gateway" -ErrorAction SilentlyContinue)) {
    try {
        Disable-ScheduledTask -TaskName "OpenClaw Gateway" | Out-Null
    } catch {
        # no-op
    }
}

Start-ScheduledTask -TaskName $autostartName
if (Get-ScheduledTask -TaskName $healthName -ErrorAction SilentlyContinue) {
    Start-ScheduledTask -TaskName $healthName
} else {
    schtasks /Run /TN $healthName *> $null
}

Write-Host "Installed tasks:"
Get-ScheduledTask -TaskName $autostartName, $healthName -ErrorAction SilentlyContinue |
    Select-Object TaskName, State |
    Format-Table -AutoSize

if (Get-ScheduledTask -TaskName "OpenClaw Gateway" -ErrorAction SilentlyContinue) {
    Write-Host ""
    Write-Host "Legacy OpenClaw task:"
    Get-ScheduledTask -TaskName "OpenClaw Gateway" |
        Select-Object TaskName, State |
        Format-Table -AutoSize
}
