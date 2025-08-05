# River Stress Test Script

param (
    [Parameter(Mandatory=$false)]
    [int]$Iterations = 100,
    
    [Parameter(Mandatory=$false)]
    [string]$DataDir = ".\data",
    
    [Parameter(Mandatory=$false)]
    [string]$HttpAddr = ":8080",
    
    [Parameter(Mandatory=$false)]
    [int]$OperationsPerIteration = 1000
)

$serverBin = Join-Path $PSScriptRoot "..\bin\server.exe"
$benchmarkBin = Join-Path $PSScriptRoot "..\bin\benchmark.exe"
$pidFile = Join-Path $DataDir "river.pid"

# Ensure data directory exists
if (-not (Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

function Start-Server {
    Write-Host "Starting River server..."
    $process = Start-Process -FilePath $serverBin -ArgumentList "-data-dir", $DataDir, "-http-addr", $HttpAddr -PassThru -NoNewWindow
    $process.Id | Out-File -FilePath $pidFile
    Write-Host "River server started (PID: $($process.Id))"
    
    # Wait for server to start
    Start-Sleep -Seconds 2
    return $process.Id
}

function Stop-Server {
    param (
        [Parameter(Mandatory=$true)]
        [int]$Pid
    )
    
    Write-Host "Stopping River server (PID: $Pid)..."
    Stop-Process -Id $Pid -Force
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    Write-Host "River server stopped"
}

function Kill-Server {
    param (
        [Parameter(Mandatory=$true)]
        [int]$Pid
    )
    
    Write-Host "Killing River server (PID: $Pid)..."
    Stop-Process -Id $Pid -Force
    Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    Write-Host "River server killed"
}

function Run-Benchmark {
    param (
        [Parameter(Mandatory=$true)]
        [int]$Operations
    )
    
    Write-Host "Running benchmark with $Operations operations..."
    & $benchmarkBin -server "http://localhost:8080" -inserts $Operations -queries 100 -threads 4 -report-interval 500
}

function Verify-Data {
    param (
        [Parameter(Mandatory=$true)]
        [int]$Operations
    )
    
    Write-Host "Verifying data consistency..."
    $errors = 0
    
    for ($i = 0; $i -lt $Operations; $i++) {
        $key = "key-$i"
        $url = "http://localhost:8080/get?key=$key"
        
        try {
            $response = Invoke-WebRequest -Uri $url -Method Get -ErrorAction SilentlyContinue
            if ($response.StatusCode -ne 200) {
                Write-Host "Error: Key $key not found (status code: $($response.StatusCode))"
                $errors++
            }
        } catch {
            Write-Host "Error: Failed to get key $key: $($_.Exception.Message)"
            $errors++
        }
        
        if ($i % 100 -eq 0) {
            Write-Host "Verified $i keys..."
        }
    }
    
    if ($errors -eq 0) {
        Write-Host "Data verification successful! All keys are present."
    } else {
        Write-Host "Data verification failed! $errors keys are missing."
    }
    
    return $errors
}

# Main stress test loop
$totalErrors = 0

for ($i = 1; $i -le $Iterations; $i++) {
    Write-Host "`n=== Stress Test Iteration $i/$Iterations ===`n"
    
    # Start server
    $pid = Start-Server
    
    # Run benchmark
    Run-Benchmark -Operations $OperationsPerIteration
    
    # Kill server abruptly to simulate crash
    Kill-Server -Pid $pid
    
    # Wait a moment
    Start-Sleep -Seconds 2
    
    # Start server again (should recover)
    $pid = Start-Server
    
    # Verify data
    $errors = Verify-Data -Operations $OperationsPerIteration
    $totalErrors += $errors
    
    # Stop server cleanly
    Stop-Server -Pid $pid
    
    # Wait a moment before next iteration
    Start-Sleep -Seconds 2
}

Write-Host "`n=== Stress Test Summary ===`n"
Write-Host "Total iterations: $Iterations"
Write-Host "Total operations: $($Iterations * $OperationsPerIteration)"
Write-Host "Total errors: $totalErrors"

if ($totalErrors -eq 0) {
    Write-Host "STRESS TEST PASSED! No data loss detected."
} else {
    Write-Host "STRESS TEST FAILED! $totalErrors keys were lost during recovery."
}