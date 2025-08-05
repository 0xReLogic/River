# River Architecture

This document provides a detailed overview of the River database architecture.

## Table of Contents

1. [Overview](#overview)
2. [Storage Engine](#storage-engine)
3. [LSM Tree](#lsm-tree)
4. [Write-Ahead Log (WAL)](#write-ahead-log-wal)
5. [Checkpoint Mechanism](#checkpoint-mechanism)
6. [Compaction](#compaction)
7. [Manifest File](#manifest-file)
8. [HTTP Server](#http-server)

## Overview

River is a high-performance columnar OLAP database engine written in pure Go. It is designed for real-time analytics workloads with direct Kafka ingestion and lightning-fast query execution.

The architecture consists of several key components:

- **Storage Engine**: Manages data storage and retrieval
- **LSM Tree**: Organizes data in a log-structured merge tree
- **WAL**: Ensures durability in case of crashes
- **Checkpoint**: Speeds up recovery after crashes
- **Compaction**: Merges data files in the background
- **Manifest**: Tracks the state of the LSM tree
- **HTTP Server**: Provides a REST API for data operations

## Storage Engine

The storage engine is the core component of River. It integrates the LSM tree, WAL, and compaction manager to provide a unified interface for data operations.

### Key Components

- **Memory Table**: In-memory key-value store for recent writes
- **Immutable Tables**: Read-only snapshots of the memory table
- **Block Cache**: Caches frequently accessed blocks
- **Block Index**: Maps keys to block locations

### Data Flow

1. Writes go to the memory table and WAL
2. When the memory table reaches a certain size, it's flushed to disk as an immutable table
3. Reads check the memory table first, then the immutable tables
4. Compaction merges immutable tables in the background

## LSM Tree

River uses a Log-Structured Merge (LSM) tree to organize data on disk. The LSM tree consists of multiple levels (L0-L6), with each level containing sorted data files.

### Level Structure

- **L0**: Contains recently flushed memory tables, may have overlapping key ranges
- **L1-L6**: Contains sorted data files with non-overlapping key ranges

### Key Features

- **Sorted Data**: Each level (except L0) contains sorted data
- **Size Ratio**: Each level is ~10x larger than the previous level
- **Compaction**: Data is moved from higher levels to lower levels via compaction

## Write-Ahead Log (WAL)

The WAL ensures durability by recording all write operations before they're applied to the memory table. In case of a crash, the WAL can be replayed to recover the memory table.

### WAL Entry Format

- **Timestamp**: When the entry was created
- **Operation Type**: Put or Delete
- **Key**: The key being modified
- **Value**: The new value (for Put operations)

### WAL File Format

- **Header**: CRC32 + Entry Size
- **Data**: Serialized WAL entry

### Recovery Process

1. Open all WAL files in chronological order
2. Replay each entry to reconstruct the memory table
3. Skip entries that are older than the last checkpoint

## Checkpoint Mechanism

The checkpoint mechanism speeds up recovery by periodically saving the state of the memory table to disk. After a crash, the memory table can be loaded from the checkpoint, and only WAL entries after the checkpoint need to be replayed.

### Checkpoint Data

- **Timestamp**: When the checkpoint was created
- **Last WAL Timestamp**: Timestamp of the last WAL entry included in the checkpoint
- **Memory Table**: Snapshot of the memory table
- **Memory Table Size**: Size of the memory table in bytes

### Checkpoint Process

1. Create a snapshot of the memory table
2. Save the snapshot to a temporary file
3. Atomically rename the temporary file to the checkpoint file

### Recovery with Checkpoint

1. Load the memory table from the checkpoint
2. Replay WAL entries after the last WAL timestamp in the checkpoint

## Compaction

Compaction is the process of merging multiple data files into a single file. This reduces the number of files and improves read performance.

### Compaction Strategies

- **Size-Tiered**: Merge files of similar size
- **Leveled**: Merge files from one level to the next

### Compaction Process

1. Select files to compact based on the compaction strategy
2. Read the selected files and merge their contents
3. Write the merged data to a new file
4. Atomically update the manifest to reference the new file
5. Delete the old files

### Compaction Metrics

- **Compaction Count**: Number of compactions performed
- **Blocks Compacted**: Number of blocks compacted
- **Bytes Read/Written**: Amount of data read/written during compaction
- **CPU Usage**: Percentage of CPU used during compaction
- **Throughput**: Bytes processed per second

## Manifest File

The manifest file tracks the state of the LSM tree, including the files in each level and their metadata.

### Manifest Data

- **Timestamp**: When the manifest was created
- **Levels**: Information about each level in the LSM tree
- **Files**: Information about each file in each level
- **Current WAL**: Path to the current WAL file
- **Last Checkpoint**: Timestamp of the last checkpoint

### File Metadata

- **Path**: Path to the file
- **Size**: Size of the file in bytes
- **Timestamp**: When the file was created
- **Min/Max Key**: Key range covered by the file
- **Entry Count**: Number of entries in the file

## HTTP Server

The HTTP server provides a REST API for data operations. It handles requests for putting, getting, and deleting data, as well as retrieving server statistics.

### Endpoints

- **GET /get?key=...**: Get the value for a key
- **POST /put?key=...**: Put a key-value pair
- **DELETE /delete?key=...**: Delete a key
- **GET /stats**: Get server statistics
- **GET /health**: Check server health

### Request Handling

1. Parse the request parameters
2. Perform the requested operation on the storage engine
3. Return the result to the client

### Graceful Restart

The server supports graceful restart, which allows it to be restarted without dropping connections:

1. Start a new process
2. The new process signals the old process when it's ready
3. The old process stops accepting new connections
4. The old process waits for existing connections to complete
5. The old process exits