# River Usage Guide

This document provides detailed instructions on how to use River, a high-performance columnar OLAP database engine.

## Table of Contents

1. [Installation](#installation)
2. [Server Management](#server-management)
3. [Data Operations](#data-operations)
4. [Performance Tuning](#performance-tuning)
5. [Monitoring](#monitoring)
6. [Troubleshooting](#troubleshooting)

## Installation

### Prerequisites

- Go 1.18+
- Make (optional, for using the Makefile)

### Building from Source

Clone the repository and build the binaries:

```bash
git clone https://github.com/0xReLogic/river.git
cd river
make build
```

This will create the following binaries in the `bin` directory:
- `server`: The River database server
- `benchmark`: A tool for benchmarking performance

## Server Management

### Starting the Server

Start the server with default settings:

```bash
bin/server
```

Or with custom settings:

```bash
bin/server -data-dir /path/to/data -http-addr :8080
```

### Using the Management Script

River comes with a PowerShell script for managing the server:

```powershell
# Start the server
powershell -File scripts/river.ps1 -Command start -DataDir ./data -HttpAddr :8080

# Check server status
powershell -File scripts/river.ps1 -Command status

# Restart the server (graceful)
powershell -File scripts/river.ps1 -Command restart

# Stop the server
powershell -File scripts/river.ps1 -Command stop
```

### Server Configuration

The server accepts the following command-line flags:

- `-data-dir`: Directory for storing data (default: `./data`)
- `-http-addr`: HTTP server address (default: `:8080`)

## Data Operations

River provides a simple HTTP API for data operations.

### Putting Data

```bash
curl -X POST "http://localhost:8080/put?key=mykey" -d "myvalue"
```

### Getting Data

```bash
curl "http://localhost:8080/get?key=mykey"
```

### Deleting Data

```bash
curl -X DELETE "http://localhost:8080/delete?key=mykey"
```

### Getting Server Statistics

```bash
curl "http://localhost:8080/stats"
```

## Performance Tuning

### Memory Usage

The memory table size can be adjusted in the code. By default, it's set to 32MB:

```go
maxMemTableSize: 32 * 1024 * 1024, // 32MB
```

Increasing this value can improve write performance but will use more memory.

### Compaction

Compaction is performed automatically in the background. The number of compaction workers can be adjusted:

```go
compaction := NewCompactionManager(lsm, dataDir, 4) // 4 worker goroutines
```

Increasing the number of workers can speed up compaction but will use more CPU.

### Checkpointing

Checkpoints are created periodically to speed up recovery. The checkpoint interval can be adjusted:

```go
checkpointInterval: 500 * time.Millisecond, // Checkpoint every 500ms
```

More frequent checkpoints will speed up recovery but may impact performance.

## Monitoring

### Server Statistics

The server provides statistics via the `/stats` endpoint:

```bash
curl "http://localhost:8080/stats"
```

This returns a JSON object with various statistics, including:

- Compaction statistics (count, bytes read/written, CPU usage)
- Memory table size
- LSM tree level statistics

### Health Check

A simple health check endpoint is available:

```bash
curl "http://localhost:8080/health"
```

## Benchmarking

River includes a benchmarking tool for measuring performance:

```bash
bin/benchmark -server http://localhost:8080 -inserts 100000 -queries 1000 -threads 4
```

Options:
- `-server`: Server address (default: `http://localhost:8080`)
- `-inserts`: Number of inserts to perform (default: `1000000`)
- `-queries`: Number of queries to perform (default: `1000`)
- `-threads`: Number of threads (default: `4`)
- `-value-size`: Size of values in bytes (default: `100`)
- `-report-interval`: Report progress every N operations (default: `1000`)

## Stress Testing

River includes a stress test script for testing crash recovery:

```bash
powershell -File scripts/stress-test.ps1 -Iterations 10 -OperationsPerIteration 1000
```

Options:
- `-Iterations`: Number of crash-recovery cycles to perform (default: `100`)
- `-DataDir`: Directory for storing data (default: `./data`)
- `-HttpAddr`: HTTP server address (default: `:8080`)
- `-OperationsPerIteration`: Number of operations per iteration (default: `1000`)

## Troubleshooting

### Server Won't Start

Check if another instance is already running:

```bash
powershell -File scripts/river.ps1 -Command status
```

Check if the data directory exists and is writable:

```bash
mkdir -p ./data
```

### Slow Performance

If you're experiencing slow performance, try:

1. Increasing the memory table size
2. Increasing the number of compaction workers
3. Adjusting the checkpoint interval

### Data Loss After Crash

River uses WAL (Write-Ahead Log) and checkpoints to prevent data loss. If you're experiencing data loss after a crash:

1. Check if the WAL directory exists and is writable
2. Check if the checkpoint directory exists and is writable
3. Run the stress test to verify crash recovery is working correctly

### High CPU Usage

If you're experiencing high CPU usage, it may be due to compaction. Try:

1. Reducing the number of compaction workers
2. Monitoring CPU usage with the `/stats` endpoint
3. Scheduling compaction during off-peak hours