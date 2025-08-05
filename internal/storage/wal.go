package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WAL (Write-Ahead Log) provides durability guarantees by logging
// operations before they are applied to the main data structure.
type WAL struct {
	// Directory where WAL files are stored
	walDir string

	// Current WAL file
	file *os.File

	// Buffered writer for the WAL file
	writer *bufio.Writer

	// Mutex to protect concurrent access
	mu sync.Mutex

	// Current WAL file size
	size int64

	// Maximum size of a WAL file before rotation
	maxSize int64

	// CRC32 table for checksums
	crc32Table *crc32.Table
}

// WALEntry represents a single entry in the WAL
type WALEntry struct {
	// Timestamp of the entry
	Timestamp int64

	// Type of operation (e.g., PUT, DELETE)
	OpType byte

	// Key and value
	Key, Value []byte
}

// WAL operation types
const (
	OpTypePut    byte = 1
	OpTypeDelete byte = 2
)

// NewWAL creates a new WAL with the given directory
func NewWAL(walDir string) (*WAL, error) {
	// Create WAL directory if it doesn't exist
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	wal := &WAL{
		walDir:     walDir,
		maxSize:    64 * 1024 * 1024, // 64MB
		crc32Table: crc32.MakeTable(crc32.Castagnoli),
	}

	// Create or open the current WAL file
	if err := wal.openCurrentFile(); err != nil {
		return nil, err
	}

	return wal, nil
}

// openCurrentFile opens the current WAL file or creates a new one
func (w *WAL) openCurrentFile() error {
	// Find the latest WAL file or create a new one
	files, err := os.ReadDir(w.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var latestFile string
	var latestTime int64

	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".wal" {
			continue
		}

		// Parse timestamp from filename
		var timestamp int64
		_, err := fmt.Sscanf(file.Name(), "%d.wal", &timestamp)
		if err != nil {
			continue
		}

		if timestamp > latestTime {
			latestTime = timestamp
			latestFile = file.Name()
		}
	}

	var path string
	if latestFile == "" {
		// Create a new WAL file
		path = filepath.Join(w.walDir, fmt.Sprintf("%d.wal", time.Now().UnixNano()))
		w.size = 0
	} else {
		// Open the latest WAL file
		path = filepath.Join(w.walDir, latestFile)
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("failed to stat WAL file: %w", err)
		}
		w.size = info.Size()
	}

	// Open the file for appending
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)

	return nil
}

// AppendPut appends a PUT operation to the WAL
func (w *WAL) AppendPut(key, value []byte) error {
	return w.append(OpTypePut, key, value)
}

// AppendDelete appends a DELETE operation to the WAL
func (w *WAL) AppendDelete(key []byte) error {
	return w.append(OpTypeDelete, key, nil)
}

// append appends an operation to the WAL
func (w *WAL) append(opType byte, key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if we need to rotate the WAL file
	if w.size >= w.maxSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	// Create WAL entry
	entry := WALEntry{
		Timestamp: time.Now().UnixNano(),
		OpType:    opType,
		Key:       key,
		Value:     value,
	}

	// Calculate entry size
	entrySize := 8 + 1 + 4 + len(key) + 4
	if value != nil {
		entrySize += len(value)
	}

	// Write entry header
	// - 4 bytes: CRC32 (calculated later)
	// - 4 bytes: Entry size
	// - 8 bytes: Timestamp
	// - 1 byte:  Operation type
	// - 4 bytes: Key length
	// - N bytes: Key
	// - 4 bytes: Value length (if PUT)
	// - M bytes: Value (if PUT)

	// Prepare buffer for the entry
	buf := make([]byte, entrySize+8) // +8 for CRC32 and entry size

	// Skip CRC32 for now (first 4 bytes)
	offset := 4

	// Entry size
	binary.LittleEndian.PutUint32(buf[offset:], uint32(entrySize))
	offset += 4

	// Timestamp
	binary.LittleEndian.PutUint64(buf[offset:], uint64(entry.Timestamp))
	offset += 8

	// Operation type
	buf[offset] = entry.OpType
	offset++

	// Key length
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(entry.Key)))
	offset += 4

	// Key
	copy(buf[offset:], entry.Key)
	offset += len(entry.Key)

	// Value length and value (if PUT)
	if entry.OpType == OpTypePut {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(entry.Value)))
		offset += 4

		copy(buf[offset:], entry.Value)
		offset += len(entry.Value)
	} else {
		// For DELETE, value length is 0
		binary.LittleEndian.PutUint32(buf[offset:], 0)
		offset += 4
	}

	// Calculate CRC32 (excluding the CRC32 field itself)
	crc := crc32.Checksum(buf[4:offset], w.crc32Table)
	binary.LittleEndian.PutUint32(buf[0:], crc)

	// Write the entry to the WAL file
	n, err := w.writer.Write(buf[:offset])
	if err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update WAL file size
	w.size += int64(n)

	// Flush to disk
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	// Sync to disk for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// rotate rotates the WAL file
func (w *WAL) rotate() error {
	// Close current file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Open a new WAL file
	return w.openCurrentFile()
}

// Replay replays the WAL entries and applies them to the given callback function
func (w *WAL) Replay(callback func(entry WALEntry) error) error {
	return w.ReplayFrom(0, callback)
}

// ReplayFrom replays the WAL entries from the given timestamp and applies them to the given callback function
func (w *WAL) ReplayFrom(fromTimestamp int64, callback func(entry WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any pending writes
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush WAL: %w", err)
	}

	// List all WAL files
	files, err := os.ReadDir(w.walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	// Sort WAL files by timestamp
	type walFile struct {
		path      string
		timestamp int64
	}

	var walFiles []walFile
	for _, file := range files {
		if file.IsDir() || filepath.Ext(file.Name()) != ".wal" {
			continue
		}

		// Parse timestamp from filename
		var timestamp int64
		_, err := fmt.Sscanf(file.Name(), "%d.wal", &timestamp)
		if err != nil {
			continue
		}

		// Skip files that are older than the checkpoint
		if timestamp < fromTimestamp {
			continue
		}

		walFiles = append(walFiles, walFile{
			path:      filepath.Join(w.walDir, file.Name()),
			timestamp: timestamp,
		})
	}

	// Sort by timestamp (oldest first)
	for i := 0; i < len(walFiles); i++ {
		for j := i + 1; j < len(walFiles); j++ {
			if walFiles[i].timestamp > walFiles[j].timestamp {
				walFiles[i], walFiles[j] = walFiles[j], walFiles[i]
			}
		}
	}

	// Replay each WAL file
	for _, file := range walFiles {
		if err := w.replayFileFrom(file.path, fromTimestamp, callback); err != nil {
			return err
		}
	}

	return nil
}

// replayFile replays a single WAL file
func (w *WAL) replayFile(path string, callback func(entry WALEntry) error) error {
	return w.replayFileFrom(path, 0, callback)
}

// replayFileFrom replays a single WAL file from the given timestamp
func (w *WAL) replayFileFrom(path string, fromTimestamp int64, callback func(entry WALEntry) error) error {
	// Open the WAL file for reading
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		// Read entry header
		// - 4 bytes: CRC32
		// - 4 bytes: Entry size
		header := make([]byte, 8)
		_, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read WAL entry header: %w", err)
		}

		// Parse header
		crc := binary.LittleEndian.Uint32(header[0:])
		entrySize := binary.LittleEndian.Uint32(header[4:])

		// Read entry data
		data := make([]byte, entrySize)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return fmt.Errorf("failed to read WAL entry data: %w", err)
		}

		// Verify CRC32
		computedCRC := crc32.Checksum(data, w.crc32Table)
		if computedCRC != crc {
			return fmt.Errorf("WAL entry corrupted: CRC mismatch")
		}

		// Parse entry
		var entry WALEntry
		offset := 0

		// Timestamp
		entry.Timestamp = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// Skip entries that are older than the checkpoint
		if entry.Timestamp <= fromTimestamp {
			// Skip the rest of this entry and continue to the next one
			continue
		}

		// Operation type
		entry.OpType = data[offset]
		offset++

		// Key length
		keyLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// Key
		entry.Key = make([]byte, keyLen)
		copy(entry.Key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// Value length
		valueLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		// Value (if present)
		if valueLen > 0 {
			entry.Value = make([]byte, valueLen)
			copy(entry.Value, data[offset:offset+int(valueLen)])
		}

		// Apply the entry
		if err := callback(entry); err != nil {
			return fmt.Errorf("failed to apply WAL entry: %w", err)
		}
	}

	return nil
}

// Close closes the WAL and releases resources
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush WAL: %w", err)
		}
	}

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return fmt.Errorf("failed to close WAL file: %w", err)
		}
	}

	return nil
}
