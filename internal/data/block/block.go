package block

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// DataType defines the type of data stored in a column block.
type DataType uint8

const (
	Int32 DataType = iota
	Int64
	Float32
	Float64
	String
	Bool
)

// CompressionType defines the compression algorithm used.
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionLZ4
)

// Header defines the metadata for a column block.
// It's a fixed-size structure.
type Header struct {
	DataType        DataType
	CompressionType CompressionType
	Count           uint32   // Number of values in the block
	RawSizeBytes    uint32   // Size of the data in bytes before compression
	StoredSizeBytes uint32   // Size of the data in bytes after compression
	CreatedAt       int64    // Unix timestamp when the block was created
	BlockID         [32]byte // SHA-256 hash of the block contents
}

// Stats stores summary statistics for the data in the block.
// This is used for query optimization (e.g., predicate pushdown).
type Stats struct {
	Min, Max uint64 // Using uint64 to generically represent min/max for numeric types
	MinKey   []byte // Minimum key in the block
	MaxKey   []byte // Maximum key in the block
}

// Block represents a single columnar block on disk.
// Layout:
// [Header]
// [Stats]
// [Data]
type Block struct {
	Header Header
	Stats  Stats
	Data   []byte

	// Key-value pairs for storage engine
	pairs   []keyValuePair
	pairsMu sync.RWMutex

	// Buffer for reading
	buffer *bytes.Buffer
}

// keyValuePair represents a key-value pair in the block
type keyValuePair struct {
	key   []byte
	value []byte
}

// NewBlock creates a new empty block
func NewBlock() *Block {
	return &Block{
		Header: Header{
			CreatedAt: time.Now().Unix(),
		},
		Stats:  Stats{},
		pairs:  make([]keyValuePair, 0),
		buffer: new(bytes.Buffer),
	}
}

// Add adds a key-value pair to the block
func (b *Block) Add(key, value []byte) error {
	b.pairsMu.Lock()
	defer b.pairsMu.Unlock()

	// Add the pair to the list
	b.pairs = append(b.pairs, keyValuePair{
		key:   key,
		value: value,
	})

	// Update min/max keys
	if len(b.Stats.MinKey) == 0 || bytes.Compare(key, b.Stats.MinKey) < 0 {
		b.Stats.MinKey = make([]byte, len(key))
		copy(b.Stats.MinKey, key)
	}

	if len(b.Stats.MaxKey) == 0 || bytes.Compare(key, b.Stats.MaxKey) > 0 {
		b.Stats.MaxKey = make([]byte, len(key))
		copy(b.Stats.MaxKey, key)
	}

	return nil
}

// Get retrieves a value for a key from the block
func (b *Block) Get(key []byte) ([]byte, error) {
	b.pairsMu.RLock()
	defer b.pairsMu.RUnlock()

	// Linear search for the key
	for _, pair := range b.pairs {
		if bytes.Equal(pair.key, key) {
			return pair.value, nil
		}
	}

	return nil, fmt.Errorf("key not found")
}

// Finalize prepares the block for writing to disk
func (b *Block) Finalize() error {
	b.pairsMu.Lock()
	defer b.pairsMu.Unlock()

	// Sort pairs by key
	sort.Slice(b.pairs, func(i, j int) bool {
		return bytes.Compare(b.pairs[i].key, b.pairs[j].key) < 0
	})

	// Reset buffer
	b.buffer.Reset()

	// Write number of pairs
	count := uint32(len(b.pairs))
	if err := binary.Write(b.buffer, binary.LittleEndian, count); err != nil {
		return fmt.Errorf("failed to write pair count: %w", err)
	}

	// Write each pair
	for _, pair := range b.pairs {
		// Write key length
		keyLen := uint32(len(pair.key))
		if err := binary.Write(b.buffer, binary.LittleEndian, keyLen); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}

		// Write key
		if _, err := b.buffer.Write(pair.key); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}

		// Write value length
		valueLen := uint32(len(pair.value))
		if err := binary.Write(b.buffer, binary.LittleEndian, valueLen); err != nil {
			return fmt.Errorf("failed to write value length: %w", err)
		}

		// Write value
		if _, err := b.buffer.Write(pair.value); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}

	// Update header
	b.Header.Count = count
	b.Header.RawSizeBytes = uint32(b.buffer.Len())
	b.Header.StoredSizeBytes = b.Header.RawSizeBytes // No compression yet

	// Copy buffer to data
	b.Data = make([]byte, b.buffer.Len())
	copy(b.Data, b.buffer.Bytes())

	// Calculate block ID (SHA-256 hash of data)
	b.Header.BlockID = sha256.Sum256(b.Data)

	return nil
}

// Encode writes the block to the given writer.
func (b *Block) Encode(w io.Writer) error {
	// Finalize if not already done
	if len(b.Data) == 0 {
		if err := b.Finalize(); err != nil {
			return err
		}
	}

	// Write header
	if err := binary.Write(w, binary.LittleEndian, &b.Header); err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	}

	// Write stats (only fixed-size fields)
	if err := binary.Write(w, binary.LittleEndian, b.Stats.Min); err != nil {
		return fmt.Errorf("failed to write block stats min: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, b.Stats.Max); err != nil {
		return fmt.Errorf("failed to write block stats max: %w", err)
	}

	// Write min key length and min key
	minKeyLen := uint32(len(b.Stats.MinKey))
	if err := binary.Write(w, binary.LittleEndian, minKeyLen); err != nil {
		return fmt.Errorf("failed to write min key length: %w", err)
	}
	if minKeyLen > 0 {
		if _, err := w.Write(b.Stats.MinKey); err != nil {
			return fmt.Errorf("failed to write min key: %w", err)
		}
	}

	// Write max key length and max key
	maxKeyLen := uint32(len(b.Stats.MaxKey))
	if err := binary.Write(w, binary.LittleEndian, maxKeyLen); err != nil {
		return fmt.Errorf("failed to write max key length: %w", err)
	}
	if maxKeyLen > 0 {
		if _, err := w.Write(b.Stats.MaxKey); err != nil {
			return fmt.Errorf("failed to write max key: %w", err)
		}
	}

	// Write data
	_, err := w.Write(b.Data)
	if err != nil {
		return fmt.Errorf("failed to write block data: %w", err)
	}

	return nil
}

// Decode reads a block from the given reader.
func (b *Block) Decode(r io.Reader) error {
	// Read header
	if err := binary.Read(r, binary.LittleEndian, &b.Header); err != nil {
		return fmt.Errorf("failed to read block header: %w", err)
	}

	// Read stats (only fixed-size fields)
	if err := binary.Read(r, binary.LittleEndian, &b.Stats.Min); err != nil {
		return fmt.Errorf("failed to read block stats min: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &b.Stats.Max); err != nil {
		return fmt.Errorf("failed to read block stats max: %w", err)
	}

	// Read min key length and min key
	var minKeyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &minKeyLen); err != nil {
		return fmt.Errorf("failed to read min key length: %w", err)
	}
	if minKeyLen > 0 {
		b.Stats.MinKey = make([]byte, minKeyLen)
		if _, err := io.ReadFull(r, b.Stats.MinKey); err != nil {
			return fmt.Errorf("failed to read min key: %w", err)
		}
	}

	// Read max key length and max key
	var maxKeyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &maxKeyLen); err != nil {
		return fmt.Errorf("failed to read max key length: %w", err)
	}
	if maxKeyLen > 0 {
		b.Stats.MaxKey = make([]byte, maxKeyLen)
		if _, err := io.ReadFull(r, b.Stats.MaxKey); err != nil {
			return fmt.Errorf("failed to read max key: %w", err)
		}
	}

	// Read data
	b.Data = make([]byte, b.Header.StoredSizeBytes)
	_, err := io.ReadFull(r, b.Data)
	if err != nil {
		return fmt.Errorf("failed to read block data: %w", err)
	}

	// Parse key-value pairs from data
	b.buffer = bytes.NewBuffer(b.Data)

	// Read number of pairs
	var count uint32
	if err := binary.Read(b.buffer, binary.LittleEndian, &count); err != nil {
		return fmt.Errorf("failed to read pair count: %w", err)
	}

	// Read each pair
	b.pairs = make([]keyValuePair, count)
	for i := uint32(0); i < count; i++ {
		// Read key length
		var keyLen uint32
		if err := binary.Read(b.buffer, binary.LittleEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %w", err)
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(b.buffer, key); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}

		// Read value length
		var valueLen uint32
		if err := binary.Read(b.buffer, binary.LittleEndian, &valueLen); err != nil {
			return fmt.Errorf("failed to read value length: %w", err)
		}

		// Read value
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(b.buffer, value); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}

		// Store the pair
		b.pairs[i] = keyValuePair{
			key:   key,
			value: value,
		}
	}

	return nil
}

// ID returns the unique identifier for the block
func (b *Block) ID() string {
	return hex.EncodeToString(b.Header.BlockID[:])
}

// MinKey returns the minimum key in the block
func (b *Block) MinKey() string {
	return string(b.Stats.MinKey)
}

// MaxKey returns the maximum key in the block
func (b *Block) MaxKey() string {
	return string(b.Stats.MaxKey)
}

// Count returns the number of key-value pairs in the block
func (b *Block) Count() int {
	return len(b.pairs)
}

// Size returns the size of the block in bytes
func (b *Block) Size() int {
	return int(b.Header.StoredSizeBytes)
}

// Reader returns a reader for the block data
func (b *Block) Reader() io.Reader {
	return bytes.NewReader(b.Data)
}

// String returns a string representation of the block
func (b *Block) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Block ID: %s\n", b.ID()))
	sb.WriteString(fmt.Sprintf("Created: %s\n", time.Unix(b.Header.CreatedAt, 0).Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Count: %d\n", b.Header.Count))
	sb.WriteString(fmt.Sprintf("Size: %d bytes\n", b.Header.StoredSizeBytes))
	sb.WriteString(fmt.Sprintf("Min Key: %s\n", b.MinKey()))
	sb.WriteString(fmt.Sprintf("Max Key: %s\n", b.MaxKey()))

	return sb.String()
}
