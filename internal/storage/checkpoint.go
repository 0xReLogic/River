package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Checkpoint represents a snapshot of the memory table
type Checkpoint struct {
	// Path to the checkpoint file
	path string

	// Mutex to protect concurrent access
	mu sync.Mutex

	// Last WAL timestamp included in this checkpoint
	lastWALTimestamp int64
}

// CheckpointData represents the data stored in a checkpoint file
type CheckpointData struct {
	// Timestamp when the checkpoint was created
	Timestamp int64 `json:"timestamp"`

	// Last WAL entry timestamp included in this checkpoint
	LastWALTimestamp int64 `json:"last_wal_timestamp"`

	// Memory table data
	MemTable map[string][]byte `json:"mem_table"`

	// Memory table size
	MemTableSize int64 `json:"mem_table_size"`
}

// NewCheckpoint creates a new checkpoint manager
func NewCheckpoint(baseDir string) (*Checkpoint, error) {
	// Create checkpoint directory if it doesn't exist
	checkpointDir := filepath.Join(baseDir, "checkpoint")
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	return &Checkpoint{
		path: filepath.Join(checkpointDir, "checkpoint.json"),
	}, nil
}

// Save saves the current memory table to a checkpoint file
func (c *Checkpoint) Save(memTable map[string][]byte, memTableSize int64, lastWALTimestamp int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create checkpoint data
	data := CheckpointData{
		Timestamp:        time.Now().UnixNano(),
		LastWALTimestamp: lastWALTimestamp,
		MemTable:         memTable,
		MemTableSize:     memTableSize,
	}

	// Create a temporary file
	tempPath := c.path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint file: %w", err)
	}

	// Write checkpoint data to the file
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		file.Close()
		return fmt.Errorf("failed to encode checkpoint data: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync checkpoint file: %w", err)
	}

	// Close the file before renaming
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close checkpoint file: %w", err)
	}

	// Rename temporary file to checkpoint file (atomic operation)
	if err := os.Rename(tempPath, c.path); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	// Update last WAL timestamp
	c.lastWALTimestamp = lastWALTimestamp

	return nil
}

// Load loads the memory table from a checkpoint file
func (c *Checkpoint) Load() (map[string][]byte, int64, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if checkpoint file exists
	if _, err := os.Stat(c.path); os.IsNotExist(err) {
		// No checkpoint file, return empty memory table
		return make(map[string][]byte), 0, 0, nil
	} else if err != nil {
		// Other error checking file
		return nil, 0, 0, fmt.Errorf("failed to check checkpoint file: %w", err)
	}

	// Open checkpoint file
	file, err := os.Open(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			// File might have been deleted between stat and open
			return make(map[string][]byte), 0, 0, nil
		}
		return nil, 0, 0, fmt.Errorf("failed to open checkpoint file: %w", err)
	}
	defer file.Close()

	// Read checkpoint data
	var data CheckpointData
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		// If we can't decode, treat as if there's no checkpoint
		return make(map[string][]byte), 0, 0, nil
	}

	// Update last WAL timestamp
	c.lastWALTimestamp = data.LastWALTimestamp

	// If memTable is nil, create an empty one
	if data.MemTable == nil {
		data.MemTable = make(map[string][]byte)
	}

	return data.MemTable, data.MemTableSize, data.LastWALTimestamp, nil
}

// GetLastWALTimestamp returns the last WAL timestamp included in the checkpoint
func (c *Checkpoint) GetLastWALTimestamp() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastWALTimestamp
}
