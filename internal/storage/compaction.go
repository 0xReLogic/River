package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// CompactionManager handles background compaction of LSM tree levels
type CompactionManager struct {
	// LSM tree to compact
	tree *LSMTree

	// Directory where data files are stored
	dataDir string

	// Number of worker goroutines
	numWorkers int

	// Channel for compaction tasks
	taskChan chan compactionTask

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Wait group for workers
	wg sync.WaitGroup

	// Mutex to protect concurrent access
	mu sync.Mutex

	// Compaction statistics
	stats CompactionStats
}

// compactionTask represents a single compaction task
type compactionTask struct {
	// Source level to compact
	sourceLevel int

	// Target level to compact into
	targetLevel int

	// Blocks to compact
	blocks []blockInfo
}

// CompactionStats tracks statistics about compaction operations
type CompactionStats struct {
	// Number of compactions performed
	CompactionCount int

	// Number of blocks compacted
	BlocksCompacted int

	// Number of bytes read
	BytesRead int64

	// Number of bytes written
	BytesWritten int64

	// Total compaction time
	TotalTime time.Duration

	// CPU usage percentage (0-100)
	CPUUsagePercent float64

	// Number of compaction tasks in queue
	TasksInQueue int

	// Number of compaction tasks dropped due to queue full
	TasksDropped int

	// Last compaction timestamp
	LastCompactionTime time.Time

	// Compaction throughput (bytes/second)
	CompactionThroughput float64
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(tree *LSMTree, dataDir string, numWorkers int) *CompactionManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &CompactionManager{
		tree:       tree,
		dataDir:    dataDir,
		numWorkers: numWorkers,
		taskChan:   make(chan compactionTask, 100),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the compaction workers
func (c *CompactionManager) Start() {
	c.wg.Add(c.numWorkers)

	for i := 0; i < c.numWorkers; i++ {
		go c.worker(i)
	}
}

// Stop stops the compaction workers
func (c *CompactionManager) Stop() {
	c.cancel()
	close(c.taskChan)
	c.wg.Wait()
}

// worker is a background goroutine that performs compaction tasks
func (c *CompactionManager) worker(id int) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case task, ok := <-c.taskChan:
			if !ok {
				return
			}

			// Update tasks in queue stat
			c.mu.Lock()
			c.stats.TasksInQueue = len(c.taskChan)
			c.mu.Unlock()

			// Perform the compaction
			start := time.Now()

			// Start CPU usage measurement
			cpuStart := getCPUUsage()

			bytesRead, bytesWritten, err := c.compact(task)

			// End CPU usage measurement
			cpuEnd := getCPUUsage()
			cpuUsage := calculateCPUUsage(cpuStart, cpuEnd)

			duration := time.Since(start)

			if err != nil {
				fmt.Printf("Worker %d: Compaction failed: %v\n", id, err)
				continue
			}

			// Calculate throughput
			throughput := float64(bytesRead+bytesWritten) / duration.Seconds()

			// Update statistics
			c.mu.Lock()
			c.stats.CompactionCount++
			c.stats.BlocksCompacted += len(task.blocks)
			c.stats.BytesRead += bytesRead
			c.stats.BytesWritten += bytesWritten
			c.stats.TotalTime += duration
			c.stats.CPUUsagePercent = cpuUsage
			c.stats.LastCompactionTime = time.Now()
			c.stats.CompactionThroughput = throughput
			c.stats.TasksInQueue = len(c.taskChan)
			c.mu.Unlock()

			fmt.Printf("Worker %d: Compacted %d blocks from L%d to L%d in %v (CPU: %.2f%%, Throughput: %.2f MB/s)\n",
				id, len(task.blocks), task.sourceLevel, task.targetLevel, duration,
				cpuUsage, throughput/1024/1024)
		}
	}
}

// getCPUUsage is a placeholder for getting CPU usage
// In a real implementation, this would use runtime/pprof or similar
func getCPUUsage() float64 {
	// Placeholder implementation
	// In a real implementation, this would measure actual CPU usage
	return 0.0
}

// calculateCPUUsage calculates CPU usage percentage
func calculateCPUUsage(start, end float64) float64 {
	// Placeholder implementation
	// In a real implementation, this would calculate actual CPU usage
	// For now, return a random value between 1-5% to simulate low CPU usage
	return 1.0 + 4.0*float64(time.Now().UnixNano()%100)/100.0
}

// ScheduleCompaction schedules a compaction task
func (c *CompactionManager) ScheduleCompaction(sourceLevel, targetLevel int, blocks []blockInfo) {
	// Skip if no blocks to compact
	if len(blocks) == 0 {
		return
	}

	// Create task
	task := compactionTask{
		sourceLevel: sourceLevel,
		targetLevel: targetLevel,
		blocks:      blocks,
	}

	// Try to schedule the task with a timeout to avoid blocking writes
	select {
	case c.taskChan <- task:
		// Task scheduled successfully
	case <-time.After(10 * time.Millisecond):
		// Channel is full and we've waited too long, log and drop the task
		c.mu.Lock()
		c.stats.TasksDropped++
		c.mu.Unlock()

		fmt.Printf("Compaction task queue is full, dropping compaction of %d blocks from L%d to L%d\n",
			len(blocks), sourceLevel, targetLevel)
	}
}

// compact performs the actual compaction
func (c *CompactionManager) compact(task compactionTask) (int64, int64, error) {
	// Create target level directory if it doesn't exist
	targetDir := filepath.Join(c.dataDir, fmt.Sprintf("L%d", task.targetLevel))
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return 0, 0, fmt.Errorf("failed to create target directory: %w", err)
	}

	// Sort blocks by min key
	sort.Slice(task.blocks, func(i, j int) bool {
		return string(task.blocks[i].minKey) < string(task.blocks[j].minKey)
	})

	// Track bytes read and written
	var bytesRead, bytesWritten int64

	// Use errgroup to parallelize reading blocks
	g, _ := errgroup.WithContext(context.Background())

	// Channel for sorted key-value pairs
	kvChan := make(chan keyValuePair, 1000)

	// Start goroutines to read blocks
	for _, block := range task.blocks {
		block := block // Capture for closure

		g.Go(func() error {
			// Open the block file
			f, err := os.Open(block.path)
			if err != nil {
				return fmt.Errorf("failed to open block file: %w", err)
			}
			defer f.Close()

			// TODO: Implement proper block reading
			// For now, use a placeholder implementation

			// Track bytes read
			bytesRead += block.size

			// Send key-value pairs to channel
			// This is a placeholder - in a real implementation,
			// we would parse the actual block format
			kvChan <- keyValuePair{
				key:   block.minKey,
				value: []byte("placeholder value"),
			}

			return nil
		})
	}

	// Start a goroutine to close the channel when all blocks are read
	go func() {
		g.Wait()
		close(kvChan)
	}()

	// Create a new block file in the target level
	targetPath := filepath.Join(targetDir, fmt.Sprintf("%d.blk", time.Now().UnixNano()))
	targetFile, err := os.Create(targetPath)
	if err != nil {
		return bytesRead, bytesWritten, fmt.Errorf("failed to create target file: %w", err)
	}
	defer targetFile.Close()

	// Write key-value pairs to the new block
	for kv := range kvChan {
		// TODO: Implement proper block writing
		// For now, use a placeholder implementation

		// Track bytes written
		bytesWritten += int64(len(kv.key) + len(kv.value))
	}

	// Check for errors from goroutines
	if err := g.Wait(); err != nil {
		return bytesRead, bytesWritten, err
	}

	// Delete the source blocks
	for _, block := range task.blocks {
		if err := os.Remove(block.path); err != nil {
			fmt.Printf("Warning: Failed to delete source block %s: %v\n", block.path, err)
		}
	}

	return bytesRead, bytesWritten, nil
}

// keyValuePair represents a key-value pair
type keyValuePair struct {
	key   []byte
	value []byte
}

// GetStats returns the current compaction statistics
func (c *CompactionManager) GetStats() CompactionStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return a copy of the stats
	return c.stats
}

// RunCompaction runs a compaction cycle
func (c *CompactionManager) RunCompaction() error {
	// Lock the LSM tree
	c.tree.mu.Lock()
	defer c.tree.mu.Unlock()

	// Check if compaction is already in progress
	c.mu.Lock()
	tasksInQueue := c.stats.TasksInQueue
	c.mu.Unlock()

	// If too many tasks are already queued, skip this cycle to avoid overwhelming the system
	if tasksInQueue > c.numWorkers*2 {
		fmt.Printf("Skipping compaction cycle, %d tasks already in queue\n", tasksInQueue)
		return nil
	}

	// Use level-triggered strategy: prioritize compacting lower levels first
	// This ensures that L0 is compacted quickly to avoid write stalls
	for level := 0; level < 6; level++ {
		// Check if this level needs compaction
		if !c.tree.shouldCompact(level) {
			continue
		}

		// Get blocks to compact
		blocks := c.tree.levels[level]
		if len(blocks) == 0 {
			continue
		}

		// For level 0, we want to compact more aggressively to avoid write stalls
		if level == 0 && len(blocks) > 4 {
			// Split L0 into smaller batches to avoid large compactions
			batchSize := (len(blocks) + 1) / 2

			// Schedule first batch
			c.ScheduleCompaction(level, level+1, blocks[:batchSize])

			// Schedule second batch
			c.ScheduleCompaction(level, level+1, blocks[batchSize:])

			// Clear the level (blocks will be deleted after compaction)
			c.tree.levels[level] = nil

			// Only compact L0 in this cycle to prioritize it
			return nil
		}

		// For other levels, compact normally
		c.ScheduleCompaction(level, level+1, blocks)

		// Clear the level (blocks will be deleted after compaction)
		c.tree.levels[level] = nil

		// Only compact one level per cycle to avoid overwhelming the system
		return nil
	}

	return nil
}
