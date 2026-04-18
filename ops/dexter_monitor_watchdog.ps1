param(
    [string]$Root = "D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed",
    [string]$PythonExe = "C:\Python312\python.exe",
    [string]$LogFile = "D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\runtime\dexter_monitor_watchdog.log",
    [int]$StartTimeoutSec = 25
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

function Get-MonitorProcess {
    Get-CimInstance Win32_Process -Filter "name='python.exe'" |
        Where-Object {
            $cmd = [string]($_.CommandLine)
            $cmd -match "main\.py\s+monitor"
        } |
        Select-Object -First 1
}

$mainScript = Join-Path $Root "main.py"
if (-not (Test-Path $mainScript)) {
    Write-Log -Level "ERROR" -Message "main.py not found: $mainScript"
    exit 1
}

$logDir = Split-Path -Parent $LogFile
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
}
$stdoutLog = Join-Path $logDir "monitor.stdout.log"
$stderrLog = Join-Path $logDir "monitor.stderr.log"

try {
    $py = Resolve-PythonExe -Preferred $PythonExe
} catch {
    Write-Log -Level "ERROR" -Message $_.Exception.Message
    exit 1
}

$proc = Get-MonitorProcess
if ($proc) {
    Write-Log -Level "OK" -Message "Monitor healthy pid=$($proc.ProcessId)"
    exit 0
}

try {
    $p = Start-Process `
        -FilePath $py `
        -ArgumentList "main.py", "monitor" `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutLog `
        -RedirectStandardError $stderrLog `
        -PassThru
    $startedPid = [int]$p.Id
    $waitSec = [Math]::Max(6, [int]$StartTimeoutSec)
    for ($i = 0; $i -lt $waitSec; $i++) {
        Start-Sleep -Seconds 1
        $after = Get-MonitorProcess
        if ($after) {
            Write-Log -Level "INFO" -Message "Monitor restarted pid=$($after.ProcessId)"
            exit 0
        }
    }

    $alive = Get-CimInstance Win32_Process -Filter "ProcessId=$startedPid" -ErrorAction SilentlyContinue
    if ($alive) {
        Write-Log -Level "ERROR" -Message "Restart attempted (pid=$startedPid) but process not detectable after ${waitSec}s"
    } else {
        Write-Log -Level "ERROR" -Message "Restart failed: monitor exited early (pid=$startedPid)"
    }
    if (Test-Path $stderrLog) {
        $tail = Get-Content -Path $stderrLog -Tail 1 -ErrorAction SilentlyContinue
        if ($tail) {
            Write-Log -Level "ERROR" -Message "monitor stderr: $tail"
        }
    }
    exit 2
} catch {
    Write-Log -Level "ERROR" -Message "Restart failed: $($_.Exception.Message)"
    exit 2
}
