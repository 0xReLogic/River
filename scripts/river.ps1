# River Server Management Script

param (
    [Parameter(Mandatory=$true)]
    [ValidateSet("start", "stop", "restart", "status")]
    [string]$Command,
    
    [Parameter(Mandatory=$false)]
    [string]$DataDir = ".\data",
    
    [Parameter(Mandatory=$false)]
    [string]$HttpAddr = ":8080"
)

$serverBin = Join-Path $PSScriptRoot "..\bin\server.exe"
$pidFile = Join-Path $DataDir "river.pid"

# Ensure data directory exists
if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

function Get-ServerPid {
    if (Test-Path $pidFile) {
        $pid = Get-Content $pidFile
        $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
        if ($process) {
            return $pid
        }
    }
    return $null
}

function Start-Server {
    $pid = Get-ServerPid
    if ($pid) {
        Write-Host "River server is already running (PID: $pid)"
        return
    }
    
    Write-Host "Starting River server..."
    $process = Start-Process -FilePath $serverBin -ArgumentList "-data-dir", $DataDir, "-http-addr", $HttpAddr -PassThru -NoNewWindow
    $process.Id | Out-File -FilePath $pidFile
    Write-Host "River server started (PID: $($process.Id))"
}

function Stop-Server {
    $pid = Get-ServerPid
    if (-not $pid) {
        Write-Host "River server is not running"
        return
    }
    
    Write-Host "Stopping River server (PID: $pid)..."
    Stop-Process -Id $pid -Force
    Remove-Item $pidFile -Force
    Write-Host "River server stopped"
}

function Restart-Server {
    $pid = Get-ServerPid
    if ($pid) {
        Write-Host "Gracefully restarting River server (PID: $pid)..."
        $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
        if ($process) {
            # Send SIGUSR2 signal for graceful restart
            # Note: In Windows, we can't easily send signals, so we'll stop and start
            Stop-Server
            Start-Sleep -Seconds 1
            Start-Server
        } else {
            Write-Host "River server is not running, starting..."
            Start-Server
        }
    } else {
        Write-Host "River server is not running, starting..."
        Start-Server
    }
}

function Get-ServerStatus {
    $pid = Get-ServerPid
    if ($pid) {
        $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
        if ($process) {
            Write-Host "River server is running (PID: $pid)"
            Write-Host "Uptime: $((Get-Date) - $process.StartTime)"
            
            # Try to get stats from the server
            try {
                $response = Invoke-RestMethod -Uri "http://localhost:8080/stats" -Method Get
                Write-Host "Server Stats:"
                $response | Format-List
            } catch {
                Write-Host "Could not get server stats: $_"
            }
        } else {
            Write-Host "River server is not running (stale PID file found)"
            Remove-Item $pidFile -Force
        }
    } else {
        Write-Host "River server is not running"
    }
}

# Execute the requested command
switch ($Command) {
    "start" { Start-Server }
    "stop" { Stop-Server }
    "restart" { Restart-Server }
    "status" { Get-ServerStatus }
}