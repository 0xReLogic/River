package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Manifest represents the state of the LSM tree
type Manifest struct {
	// Path to the manifest file
	path string

	// Mutex to protect concurrent access
	mu sync.Mutex

	// Current manifest data
	data ManifestData
}

// ManifestData represents the data stored in a manifest file
type ManifestData struct {
	// Timestamp when the manifest was created
	Timestamp int64 `json:"timestamp"`

	// LSM tree levels
	Levels []LevelData `json:"levels"`

	// Current WAL file
	CurrentWAL string `json:"current_wal"`

	// Last checkpoint timestamp
	LastCheckpoint int64 `json:"last_checkpoint"`
}

// LevelData represents data about a level in the LSM tree
type LevelData struct {
	// Level number
	Level int `json:"level"`

	// Files in this level
	Files []FileData `json:"files"`
}

// FileData represents data about a file in the LSM tree
type FileData struct {
	// File path
	Path string `json:"path"`

	// File size
	Size int64 `json:"size"`

	// Timestamp when the file was created
	Timestamp int64 `json:"timestamp"`

	// Min key in the file
	MinKey string `json:"min_key"`

	// Max key in the file
	MaxKey string `json:"max_key"`

	// Number of entries in the file
	EntryCount int `json:"entry_count"`
}

// NewManifest creates a new manifest
func NewManifest(baseDir string) (*Manifest, error) {
	// Create manifest directory if it doesn't exist
	manifestDir := filepath.Join(baseDir, "manifest")
	if err := os.MkdirAll(manifestDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create manifest directory: %w", err)
	}

	manifest := &Manifest{
		path: filepath.Join(manifestDir, "manifest.json"),
		data: ManifestData{
			Timestamp: time.Now().UnixNano(),
			Levels:    make([]LevelData, 7), // 7 levels (0-6)
		},
	}

	// Initialize levels
	for i := 0; i < 7; i++ {
		manifest.data.Levels[i] = LevelData{
			Level: i,
			Files: make([]FileData, 0),
		}
	}

	// Load existing manifest if it exists
	if _, err := os.Stat(manifest.path); err == nil {
		if err := manifest.load(); err != nil {
			return nil, fmt.Errorf("failed to load manifest: %w", err)
		}
	}

	return manifest, nil
}

// Save saves the manifest to disk
func (m *Manifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update timestamp
	m.data.Timestamp = time.Now().UnixNano()

	// Create a temporary file
	tempPath := m.path + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create manifest file: %w", err)
	}

	// Write manifest data to the file
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(m.data); err != nil {
		file.Close()
		return fmt.Errorf("failed to encode manifest data: %w", err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("failed to sync manifest file: %w", err)
	}

	// Close the file before renaming
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close manifest file: %w", err)
	}

	// Rename temporary file to manifest file (atomic operation)
	if err := os.Rename(tempPath, m.path); err != nil {
		return fmt.Errorf("failed to rename manifest file: %w", err)
	}

	return nil
}

// load loads the manifest from disk
func (m *Manifest) load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Open manifest file
	file, err := os.Open(m.path)
	if err != nil {
		return fmt.Errorf("failed to open manifest file: %w", err)
	}
	defer file.Close()

	// Read manifest data
	var data ManifestData
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return fmt.Errorf("failed to decode manifest data: %w", err)
	}

	// Update manifest data
	m.data = data

	return nil
}

// UpdateLevel updates the files in a level
func (m *Manifest) UpdateLevel(level int, files []FileData) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate level
	if level < 0 || level >= len(m.data.Levels) {
		return fmt.Errorf("invalid level: %d", level)
	}

	// Update level
	m.data.Levels[level].Files = files

	return nil
}

// UpdateCurrentWAL updates the current WAL file
func (m *Manifest) UpdateCurrentWAL(walFile string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update current WAL
	m.data.CurrentWAL = walFile

	return nil
}

// UpdateLastCheckpoint updates the last checkpoint timestamp
func (m *Manifest) UpdateLastCheckpoint(timestamp int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Update last checkpoint
	m.data.LastCheckpoint = timestamp

	return nil
}

// GetLevelFiles returns the files in a level
func (m *Manifest) GetLevelFiles(level int) ([]FileData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate level
	if level < 0 || level >= len(m.data.Levels) {
		return nil, fmt.Errorf("invalid level: %d", level)
	}

	// Return a copy of the files
	files := make([]FileData, len(m.data.Levels[level].Files))
	copy(files, m.data.Levels[level].Files)

	return files, nil
}

// GetCurrentWAL returns the current WAL file
func (m *Manifest) GetCurrentWAL() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data.CurrentWAL
}

// GetLastCheckpoint returns the last checkpoint timestamp
func (m *Manifest) GetLastCheckpoint() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data.LastCheckpoint
}
