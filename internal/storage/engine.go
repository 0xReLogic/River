package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/0xReLogic/river/internal/data/block"
)

// Engine is the main storage engine that integrates LSM tree, WAL, and compaction
type Engine struct {
	// Base directory for all storage files
	baseDir string

	// LSM tree for data storage
	lsm *LSMTree

	// WAL for crash recovery
	wal *WAL

	// Checkpoint for faster recovery
	checkpoint *Checkpoint

	// Compaction manager for background compaction
	compaction *CompactionManager

	// Mutex to protect concurrent access
	mu sync.RWMutex

	// Memory table (not yet flushed to disk)
	memTable map[string][]byte

	// Size of the memory table in bytes
	memTableSize int64

	// Maximum size of the memory table before flushing to disk
	maxMemTableSize int64

	// Channel to signal background flushing
	flushChan chan struct{}

	// Channel to signal background checkpointing
	checkpointChan chan struct{}

	// Last WAL timestamp that was checkpointed
	lastCheckpointedWALTimestamp int64

	// Flag to indicate if the engine is closed
	closed bool

	// Checkpoint interval in milliseconds
	checkpointInterval time.Duration
}

// NewEngine creates a new storage engine
func NewEngine(baseDir string) (*Engine, error) {
	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// Create subdirectories
	dataDir := filepath.Join(baseDir, "data")
	walDir := filepath.Join(baseDir, "wal")

	// Create LSM tree
	lsm, err := NewLSMTree(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create LSM tree: %w", err)
	}

	// Create WAL
	wal, err := NewWAL(walDir)
	if err != nil {
		lsm.Close()
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Create checkpoint manager
	checkpoint, err := NewCheckpoint(baseDir)
	if err != nil {
		wal.Close()
		lsm.Close()
		return nil, fmt.Errorf("failed to create checkpoint manager: %w", err)
	}

	// Create compaction manager
	compaction := NewCompactionManager(lsm, dataDir, 4) // 4 worker goroutines

	engine := &Engine{
		baseDir:            baseDir,
		lsm:                lsm,
		wal:                wal,
		checkpoint:         checkpoint,
		compaction:         compaction,
		memTable:           make(map[string][]byte),
		maxMemTableSize:    32 * 1024 * 1024, // 32MB
		flushChan:          make(chan struct{}, 1),
		checkpointChan:     make(chan struct{}, 1),
		checkpointInterval: 500 * time.Millisecond, // Checkpoint every 500ms
	}

	// Start compaction workers
	compaction.Start()

	// Start background flushing goroutine
	go engine.backgroundFlusher()

	// Start background checkpointing goroutine
	go engine.backgroundCheckpointer()

	// Recover from checkpoint and WAL if needed
	if err := engine.recover(); err != nil {
		engine.Close()
		return nil, fmt.Errorf("failed to recover from checkpoint/WAL: %w", err)
	}

	return engine, nil
}

// recover loads the memory table from checkpoint and replays the WAL
func (e *Engine) recover() error {
	// First, try to load from checkpoint
	memTable, memTableSize, lastWALTimestamp, err := e.checkpoint.Load()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Set memory table from checkpoint
	e.memTable = memTable
	e.memTableSize = memTableSize
	e.lastCheckpointedWALTimestamp = lastWALTimestamp

	// Then, replay WAL entries after the checkpoint
	return e.wal.ReplayFrom(lastWALTimestamp, func(entry WALEntry) error {
		switch entry.OpType {
		case OpTypePut:
			e.memTable[string(entry.Key)] = entry.Value
			e.memTableSize += int64(len(entry.Key) + len(entry.Value))
		case OpTypeDelete:
			delete(e.memTable, string(entry.Key))
		}
		e.lastCheckpointedWALTimestamp = entry.Timestamp
		return nil
	})
}

// Put stores a key-value pair
func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("engine is closed")
	}

	// Append to WAL first
	if err := e.wal.AppendPut(key, value); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// Update memory table
	oldSize := int64(0)
	if oldValue, ok := e.memTable[string(key)]; ok {
		oldSize = int64(len(oldValue))
	}

	e.memTable[string(key)] = value
	e.memTableSize += int64(len(key)+len(value)) - oldSize

	// Check if memory table needs to be flushed
	if e.memTableSize >= e.maxMemTableSize {
		// Signal background flusher
		select {
		case e.flushChan <- struct{}{}:
			// Signal sent successfully
		default:
			// Channel is full, flush already queued
		}
	}

	return nil
}

// Get retrieves a value for a key
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()

	if e.closed {
		e.mu.RUnlock()
		return nil, fmt.Errorf("engine is closed")
	}

	// Check memory table first
	if value, ok := e.memTable[string(key)]; ok {
		e.mu.RUnlock()
		return value, nil
	}

	// Release read lock before querying LSM tree
	e.mu.RUnlock()

	// Check LSM tree
	return e.lsm.Read(key)
}

// Delete removes a key-value pair
func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("engine is closed")
	}

	// Append to WAL first
	if err := e.wal.AppendDelete(key); err != nil {
		return fmt.Errorf("failed to append to WAL: %w", err)
	}

	// Update memory table (use a tombstone value)
	oldSize := int64(0)
	if oldValue, ok := e.memTable[string(key)]; ok {
		oldSize = int64(len(oldValue))
	}

	// Remove from memory table
	delete(e.memTable, string(key))
	e.memTableSize -= oldSize

	return nil
}

// backgroundFlusher is a goroutine that flushes the memory table to disk
func (e *Engine) backgroundFlusher() {
	for range e.flushChan {
		if e.closed {
			return
		}

		if err := e.flush(); err != nil {
			fmt.Printf("Error flushing memory table: %v\n", err)
		}
	}
}

// backgroundCheckpointer is a goroutine that creates checkpoints periodically
func (e *Engine) backgroundCheckpointer() {
	ticker := time.NewTicker(e.checkpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Create checkpoint if engine is not closed
			if !e.closed {
				if err := e.createCheckpoint(); err != nil {
					fmt.Printf("Error creating checkpoint: %v\n", err)
				}
			}
		case <-e.checkpointChan:
			// Create checkpoint on demand
			if !e.closed {
				if err := e.createCheckpoint(); err != nil {
					fmt.Printf("Error creating checkpoint: %v\n", err)
				}
			}
		}

		if e.closed {
			return
		}
	}
}

// createCheckpoint creates a checkpoint of the current memory table
func (e *Engine) createCheckpoint() error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Create a copy of the memory table
	memTableCopy := make(map[string][]byte, len(e.memTable))
	for k, v := range e.memTable {
		memTableCopy[k] = v
	}

	// Save checkpoint
	return e.checkpoint.Save(memTableCopy, e.memTableSize, e.lastCheckpointedWALTimestamp)
}

// flush flushes the memory table to disk
func (e *Engine) flush() error {
	e.mu.Lock()

	// Create a copy of the memory table
	memTable := e.memTable

	// Reset memory table
	e.memTable = make(map[string][]byte)
	e.memTableSize = 0

	e.mu.Unlock()

	// Convert memory table to a block
	b := block.NewBlock()

	// Add all key-value pairs to the block
	for key, value := range memTable {
		if err := b.Add([]byte(key), value); err != nil {
			return fmt.Errorf("failed to add key-value pair to block: %w", err)
		}
	}

	// Write the block to the LSM tree
	if err := e.lsm.Write(b); err != nil {
		return fmt.Errorf("failed to write block to LSM tree: %w", err)
	}

	return nil
}

// Close closes the storage engine and releases resources
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	// Set closed flag
	e.closed = true

	// Create final checkpoint
	if err := e.createCheckpoint(); err != nil {
		fmt.Printf("Error creating final checkpoint during close: %v\n", err)
	}

	// Flush memory table
	if err := e.flush(); err != nil {
		fmt.Printf("Error flushing memory table during close: %v\n", err)
	}

	// Close flush and checkpoint channels
	close(e.flushChan)
	close(e.checkpointChan)

	// Stop compaction workers
	e.compaction.Stop()

	// Close WAL
	if err := e.wal.Close(); err != nil {
		fmt.Printf("Error closing WAL: %v\n", err)
	}

	// Close LSM tree
	if err := e.lsm.Close(); err != nil {
		fmt.Printf("Error closing LSM tree: %v\n", err)
	}

	return nil
}

// Stats returns statistics about the storage engine
type Stats struct {
	// Memory table size
	MemTableSize int64

	// Number of keys in memory table
	MemTableKeys int

	// Compaction statistics
	CompactionStats CompactionStats

	// LSM tree level sizes
	LevelSizes [7]int64

	// LSM tree level block counts
	LevelBlocks [7]int
}

// GetStats returns statistics about the storage engine
func (e *Engine) GetStats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := Stats{
		MemTableSize:    e.memTableSize,
		MemTableKeys:    len(e.memTable),
		CompactionStats: e.compaction.GetStats(),
	}

	// Calculate level sizes and block counts
	for i := 0; i < 7; i++ {
		stats.LevelBlocks[i] = len(e.lsm.levels[i])

		for _, block := range e.lsm.levels[i] {
			stats.LevelSizes[i] += block.size
		}
	}

	return stats
}

// RunCompaction manually triggers a compaction cycle
func (e *Engine) RunCompaction() error {
	return e.compaction.RunCompaction()
}
