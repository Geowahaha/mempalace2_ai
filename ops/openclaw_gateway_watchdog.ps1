param(
    [int]$Port = 18789,
    [string]$GatewayCmd = "C:\Users\mrgeo\.openclaw\gateway.cmd",
    [string]$LogFile = "D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\runtime\openclaw_gateway_watchdog.log",
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

function Get-PortListener {
    param([int]$LocalPort)
    Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { [int]$_.LocalPort -eq $LocalPort } |
        Select-Object -First 1
}

function Get-GatewayProcess {
    Get-CimInstance Win32_Process |
        Where-Object {
            $cmd = [string]($_.CommandLine)
            ($_.Name -match "node|cmd") -and (
                $cmd -match "openclaw\\dist\\index\.js gateway run" -or
                $cmd -match "\\\.openclaw\\gateway\.cmd" -or
                $cmd -match "OPENCLAW_GATEWAY_PORT=18789"
            )
        }
}

$logDir = Split-Path -Parent $LogFile
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -Path $logDir -ItemType Directory -Force | Out-Null
}
$gatewayStdout = Join-Path $logDir "openclaw_gateway.stdout.log"
$gatewayStderr = Join-Path $logDir "openclaw_gateway.stderr.log"

if (-not (Test-Path $GatewayCmd)) {
    Write-Log -Level "ERROR" -Message "Gateway launcher missing: $GatewayCmd"
    exit 1
}

$listener = Get-PortListener -LocalPort $Port
if ($listener) {
    Write-Log -Level "OK" -Message "Gateway healthy on port=$Port pid=$($listener.OwningProcess)"
    exit 0
}

$procs = @(Get-GatewayProcess)
if ($procs.Count -gt 0) {
    foreach ($p in $procs) {
        try {
            Stop-Process -Id ([int]$p.ProcessId) -Force -ErrorAction Stop
            Write-Log -Level "WARN" -Message "Stopped stale gateway process pid=$($p.ProcessId)"
        } catch {
            Write-Log -Level "WARN" -Message "Failed stopping stale gateway pid=$($p.ProcessId): $($_.Exception.Message)"
        }
    }
}

try {
    $p = Start-Process `
        -FilePath $env:ComSpec `
        -ArgumentList "/c", "`"$GatewayCmd`"" `
        -WindowStyle Hidden `
        -RedirectStandardOutput $gatewayStdout `
        -RedirectStandardError $gatewayStderr `
        -PassThru
    $startedPid = [int]$p.Id
    $waitSec = [Math]::Max(6, [int]$StartTimeoutSec)
    for ($i = 0; $i -lt $waitSec; $i++) {
        Start-Sleep -Seconds 1
        $after = Get-PortListener -LocalPort $Port
        if ($after) {
            Write-Log -Level "INFO" -Message "Gateway restarted on port=$Port pid=$($after.OwningProcess)"
            exit 0
        }
    }

    $procAlive = Get-CimInstance Win32_Process -Filter "ProcessId=$startedPid" -ErrorAction SilentlyContinue
    if ($procAlive) {
        Write-Log -Level "ERROR" -Message "Restart attempted (pid=$startedPid) but port $Port still closed after ${waitSec}s"
    } else {
        Write-Log -Level "ERROR" -Message "Restart failed: gateway exited early (pid=$startedPid)"
    }
    if (Test-Path $gatewayStderr) {
        $tail = (Get-Content -Path $gatewayStderr -Tail 1 -ErrorAction SilentlyContinue)
        if ($tail) {
            Write-Log -Level "ERROR" -Message "gateway stderr: $tail"
        }
    }
    exit 2
} catch {
    Write-Log -Level "ERROR" -Message "Restart failed: $($_.Exception.Message)"
    exit 2
}
