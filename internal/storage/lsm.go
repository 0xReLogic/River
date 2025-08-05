package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/0xReLogic/river/internal/data/block"
)

// LSMTree implements a Log-Structured Merge Tree for efficient storage
// with level-triggered compaction.
type LSMTree struct {
	// Directory where all data files are stored
	dataDir string

	// Levels of the LSM tree (0-6)
	// Level 0: Newest data, can have overlapping key ranges
	// Level 6: Oldest data, no overlapping key ranges
	levels [7][]blockInfo

	// Mutex to protect concurrent access to the tree
	mu sync.RWMutex

	// Maximum size of each level (exponential growth)
	// Level 0: 64MB, Level 1: 256MB, Level 2: 1GB, etc.
	levelMaxSizes [7]int64

	// Compaction thresholds (when to trigger compaction)
	compactionThresholds [7]int64

	// Background compaction status
	compacting     bool
	compactionChan chan struct{}
}

// blockInfo contains metadata about a block file
type blockInfo struct {
	// Path to the block file
	path string

	// Size of the block in bytes
	size int64

	// Min and max keys in the block (for range queries)
	minKey, maxKey []byte

	// Creation time of the block
	createdAt time.Time
}

// NewLSMTree creates a new LSM tree with the given data directory
func NewLSMTree(dataDir string) (*LSMTree, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	tree := &LSMTree{
		dataDir:        dataDir,
		compactionChan: make(chan struct{}, 1),
	}

	// Initialize level sizes (exponential growth)
	// Level 0: 64MB
	// Level 1: 256MB
	// Level 2: 1GB
	// Level 3: 4GB
	// Level 4: 16GB
	// Level 5: 64GB
	// Level 6: 256GB
	base := int64(64 * 1024 * 1024) // 64MB
	for i := 0; i < 7; i++ {
		tree.levelMaxSizes[i] = base << (2 * i)                      // Multiply by 4^i
		tree.compactionThresholds[i] = tree.levelMaxSizes[i] * 3 / 4 // 75% full triggers compaction
	}

	// Load existing blocks from disk
	if err := tree.loadExistingBlocks(); err != nil {
		return nil, fmt.Errorf("failed to load existing blocks: %w", err)
	}

	return tree, nil
}

// loadExistingBlocks scans the data directory and loads existing block files
func (t *LSMTree) loadExistingBlocks() error {
	// For each level directory (L0, L1, ..., L6)
	for level := 0; level < 7; level++ {
		levelDir := filepath.Join(t.dataDir, fmt.Sprintf("L%d", level))

		// Skip if directory doesn't exist yet
		if _, err := os.Stat(levelDir); os.IsNotExist(err) {
			continue
		}

		// Read all block files in this level
		files, err := os.ReadDir(levelDir)
		if err != nil {
			return fmt.Errorf("failed to read level directory L%d: %w", level, err)
		}

		// Process each block file
		for _, file := range files {
			if file.IsDir() || filepath.Ext(file.Name()) != ".blk" {
				continue // Skip directories and non-block files
			}

			path := filepath.Join(levelDir, file.Name())
			info, err := file.Info()
			if err != nil {
				return fmt.Errorf("failed to get file info for %s: %w", path, err)
			}

			// Read block header to get min/max keys
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open block file %s: %w", path, err)
			}

			// TODO: Implement proper block header reading
			// For now, use placeholder min/max keys
			minKey := []byte(file.Name())
			maxKey := []byte(file.Name())

			f.Close()

			// Add block info to the appropriate level
			t.levels[level] = append(t.levels[level], blockInfo{
				path:      path,
				size:      info.Size(),
				minKey:    minKey,
				maxKey:    maxKey,
				createdAt: info.ModTime(),
			})
		}

		// Sort blocks by min key for faster lookups
		sort.Slice(t.levels[level], func(i, j int) bool {
			return string(t.levels[level][i].minKey) < string(t.levels[level][j].minKey)
		})
	}

	return nil
}

// Write adds a new block to the LSM tree (level 0)
func (t *LSMTree) Write(b *block.Block) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Create level 0 directory if it doesn't exist
	level0Dir := filepath.Join(t.dataDir, "L0")
	if err := os.MkdirAll(level0Dir, 0755); err != nil {
		return fmt.Errorf("failed to create L0 directory: %w", err)
	}

	// Generate a unique filename based on timestamp and block ID
	filename := fmt.Sprintf("%d_%s.blk", time.Now().UnixNano(), b.ID())
	path := filepath.Join(level0Dir, filename)

	// Create the block file
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create block file: %w", err)
	}
	defer f.Close()

	// Write the block to the file
	if err := b.Encode(f); err != nil {
		return fmt.Errorf("failed to encode block to file: %w", err)
	}

	// Get file size
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Add block info to level 0
	t.levels[0] = append(t.levels[0], blockInfo{
		path:      path,
		size:      info.Size(),
		minKey:    []byte(b.MinKey()),
		maxKey:    []byte(b.MaxKey()),
		createdAt: time.Now(),
	})

	// Check if level 0 needs compaction
	if t.shouldCompact(0) {
		// Trigger background compaction
		t.triggerCompaction()
	}

	return nil
}

// Read reads data from the LSM tree, searching through all levels
func (t *LSMTree) Read(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Search from newest to oldest (level 0 to 6)
	for level := 0; level < 7; level++ {
		// For level 0, we need to check all blocks (they may overlap)
		if level == 0 {
			// Search in reverse order (newest first)
			for i := len(t.levels[0]) - 1; i >= 0; i-- {
				block := t.levels[0][i]
				if t.keyInRange(key, block.minKey, block.maxKey) {
					value, err := t.readFromBlock(block.path, key)
					if err == nil {
						return value, nil
					}
					// If not found in this block, continue to the next one
				}
			}
		} else {
			// For levels 1-6, blocks don't overlap, so we can do binary search
			idx := t.findBlockIndex(level, key)
			if idx >= 0 {
				block := t.levels[level][idx]
				value, err := t.readFromBlock(block.path, key)
				if err == nil {
					return value, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("key not found")
}

// keyInRange checks if a key is within the given range (inclusive)
func (t *LSMTree) keyInRange(key, minKey, maxKey []byte) bool {
	return string(key) >= string(minKey) && string(key) <= string(maxKey)
}

// findBlockIndex uses binary search to find the block that may contain the key
func (t *LSMTree) findBlockIndex(level int, key []byte) int {
	blocks := t.levels[level]

	// Binary search for the block
	left, right := 0, len(blocks)-1
	for left <= right {
		mid := (left + right) / 2
		if string(key) < string(blocks[mid].minKey) {
			right = mid - 1
		} else if string(key) > string(blocks[mid].maxKey) {
			left = mid + 1
		} else {
			return mid // Key is in range of this block
		}
	}

	return -1 // Key not found in any block
}

// readFromBlock reads a value from a block file given a key
func (t *LSMTree) readFromBlock(path string, key []byte) ([]byte, error) {
	// Open the block file
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open block file: %w", err)
	}
	defer f.Close()

	// Create a new block
	b := block.NewBlock()

	// Decode the block
	if err := b.Decode(f); err != nil {
		return nil, fmt.Errorf("failed to decode block: %w", err)
	}

	// Get the value for the key
	return b.Get(key)
}

// shouldCompact checks if a level needs compaction
func (t *LSMTree) shouldCompact(level int) bool {
	// Calculate total size of blocks in this level
	var totalSize int64
	for _, block := range t.levels[level] {
		totalSize += block.size
	}

	return totalSize >= t.compactionThresholds[level]
}

// triggerCompaction triggers a background compaction if not already running
func (t *LSMTree) triggerCompaction() {
	if !t.compacting {
		t.compacting = true

		// Non-blocking send to compaction channel
		select {
		case t.compactionChan <- struct{}{}:
			// Signal sent successfully
		default:
			// Channel is full, compaction already queued
		}
	}
}

// StartCompactionWorker starts a background goroutine for compaction
func (t *LSMTree) StartCompactionWorker() {
	go func() {
		for range t.compactionChan {
			t.runCompaction()

			t.mu.Lock()
			t.compacting = false
			t.mu.Unlock()
		}
	}()
}

// runCompaction performs the actual compaction process
func (t *LSMTree) runCompaction() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Start from level 0 and work down
	for level := 0; level < 6; level++ {
		if !t.shouldCompact(level) {
			continue // This level doesn't need compaction
		}

		// Compact this level into the next level
		t.compactLevel(level)
	}
}

// compactLevel compacts a level into the next level
func (t *LSMTree) compactLevel(level int) {
	// TODO: Implement proper compaction
	// For now, just move all blocks to the next level
	nextLevel := level + 1
	nextLevelDir := filepath.Join(t.dataDir, fmt.Sprintf("L%d", nextLevel))

	// Create next level directory if it doesn't exist
	if err := os.MkdirAll(nextLevelDir, 0755); err != nil {
		fmt.Printf("Failed to create L%d directory: %v\n", nextLevel, err)
		return
	}

	// Move all blocks from current level to next level
	for _, block := range t.levels[level] {
		// Generate a new filename for the next level
		newPath := filepath.Join(nextLevelDir, filepath.Base(block.path))

		// Move the file
		if err := os.Rename(block.path, newPath); err != nil {
			fmt.Printf("Failed to move block from L%d to L%d: %v\n", level, nextLevel, err)
			continue
		}

		// Update the block info
		block.path = newPath
		t.levels[nextLevel] = append(t.levels[nextLevel], block)
	}

	// Clear the current level
	t.levels[level] = nil

	// Check if the next level now needs compaction
	if t.shouldCompact(nextLevel) {
		t.compactLevel(nextLevel)
	}
}

// Close closes the LSM tree and releases resources
func (t *LSMTree) Close() error {
	// Stop the compaction worker
	close(t.compactionChan)

	// Wait for any ongoing compaction to finish
	t.mu.Lock()
	defer t.mu.Unlock()

	return nil
}
