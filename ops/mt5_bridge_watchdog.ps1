param(
    [string]$BridgeRoot = "D:\dexter_pro_v3_fixed\_refs\OpenClaw-MT5-python-bridge",
    [string]$PythonExe = "C:\Python312\python.exe",
    [int]$Port = 18812,
    [string]$LogFile = "D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\runtime\mt5_bridge_watchdog.log",
    [int]$StartTimeoutSec = 25
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-EnvFileValue {
    param(
        [string]$Path,
        [string]$Key
    )
    if ((-not $Path) -or (-not $Key) -or (-not (Test-Path $Path))) {
        return $null
    }
    try {
        $pattern = '^\s*' + [Regex]::Escape($Key) + '\s*=\s*(.*)\s*$'
        foreach ($line in Get-Content -Path $Path -ErrorAction Stop) {
            $text = [string]$line
            if ($text -match '^\s*#') {
                continue
            }
            $m = [Regex]::Match($text, $pattern)
            if ($m.Success) {
                return ([string]$m.Groups[1].Value).Trim().Trim('"').Trim("'")
            }
        }
    } catch {
        return $null
    }
    return $null
}

function Test-EnvToggleTrue {
    param([string]$Value)
    $token = ([string]$Value).Trim().ToLowerInvariant()
    return @("1", "true", "yes", "on") -contains $token
}

function Write-Log {
    param(
        [string]$Level,
        [string]$Message
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$ts [$Level] $Message"
    Add-Content -Path $LogFile -Value $line
}

function Resolve-PythonExe {
    param([string]$Preferred)
    if ($Preferred -and (Test-Path $Preferred)) {
        return $Preferred
    }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source -and (Test-Path $cmd.Source)) {
        return $cmd.Source
    }
    throw "python executable not found. Preferred='$Preferred'"
}

function Get-BridgeProcess {
    Get-CimInstance Win32_Process -Filter "name='python.exe'" |
        Where-Object {
            $cmd = [string]($_.CommandLine)
            $cmd -like "*mt5_server.py*"
        }
}

function Get-PortListener {
    param([int]$LocalPort)
    Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { [int]$_.LocalPort -eq $LocalPort } |
        Select-Object -First 1
}

$logDir = Split-Path -Parent $LogFile
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
}
$bridgeStdout = Join-Path $logDir "mt5_bridge.stdout.log"
$bridgeStderr = Join-Path $logDir "mt5_bridge.stderr.log"
$projectRoot = Split-Path -Parent $PSScriptRoot
$envLocal = Join-Path $projectRoot ".env.local"
$mt5EnabledRaw = Get-EnvFileValue -Path $envLocal -Key "MT5_ENABLED"
$mt5AutopilotRaw = Get-EnvFileValue -Path $envLocal -Key "MT5_AUTOPILOT_ENABLED"
$mt5Enabled = Test-EnvToggleTrue -Value $mt5EnabledRaw
$mt5AutopilotEnabled = Test-EnvToggleTrue -Value $mt5AutopilotRaw
if ((Test-Path $envLocal) -and ((-not $mt5Enabled) -or (-not $mt5AutopilotEnabled))) {
    Write-Log -Level "INFO" -Message "Bridge disabled by env (.env.local): MT5_ENABLED=$mt5EnabledRaw MT5_AUTOPILOT_ENABLED=$mt5AutopilotRaw"
    exit 0
}

$bridgeScript = Join-Path $BridgeRoot "mt5_server.py"
if (-not (Test-Path $bridgeScript)) {
    Write-Log -Level "ERROR" -Message "Bridge script missing: $bridgeScript"
    exit 1
}

try {
    $py = Resolve-PythonExe -Preferred $PythonExe
} catch {
    Write-Log -Level "ERROR" -Message $_.Exception.Message
    exit 1
}

$listener = Get-PortListener -LocalPort $Port
if ($listener) {
    Write-Log -Level "OK" -Message "Bridge healthy on port=$Port pid=$($listener.OwningProcess)"
    exit 0
}

$bridgeProcs = @(Get-BridgeProcess)
if ($bridgeProcs.Count -gt 0) {
    foreach ($p in $bridgeProcs) {
        try {
            Stop-Process -Id ([int]$p.ProcessId) -Force -ErrorAction Stop
            Write-Log -Level "WARN" -Message "Stopped stale bridge process pid=$($p.ProcessId)"
        } catch {
            Write-Log -Level "WARN" -Message "Failed stopping stale pid=$($p.ProcessId): $($_.Exception.Message)"
        }
    }
}

try {
    $p = Start-Process `
        -FilePath $py `
        -ArgumentList "mt5_server.py" `
        -WorkingDirectory $BridgeRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $bridgeStdout `
        -RedirectStandardError $bridgeStderr `
        -PassThru
    $startedPid = [int]$p.Id
    $waitSec = [Math]::Max(6, [int]$StartTimeoutSec)
    for ($i = 0; $i -lt $waitSec; $i++) {
        Start-Sleep -Seconds 1
        $after = Get-PortListener -LocalPort $Port
        if ($after) {
            Write-Log -Level "INFO" -Message "Bridge restarted on port=$Port pid=$($after.OwningProcess)"
            exit 0
        }
    }
    $procAlive = Get-CimInstance Win32_Process -Filter "ProcessId=$startedPid" -ErrorAction SilentlyContinue
    if ($procAlive) {
        Write-Log -Level "ERROR" -Message "Restart attempted (pid=$startedPid) but port $Port still closed after ${waitSec}s"
    } else {
        Write-Log -Level "ERROR" -Message "Restart failed: mt5_server.py exited early (pid=$startedPid)"
    }
    if (Test-Path $bridgeStderr) {
        $tail = (Get-Content -Path $bridgeStderr -Tail 1 -ErrorAction SilentlyContinue)
        if ($tail) {
            Write-Log -Level "ERROR" -Message "mt5_server stderr: $tail"
        }
    }
    exit 2
} catch {
    Write-Log -Level "ERROR" -Message "Restart failed: $($_.Exception.Message)"
    exit 2
}
