package block

import (
	"encoding/binary"
	"fmt
	"io"
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
	Count           uint32 // Number of values in the block
	RawSizeBytes    uint32 // Size of the data in bytes before compression
	StoredSizeBytes uint32 // Size of the data in bytes after compression
}

// Stats stores summary statistics for the data in the block.
// This is used for query optimization (e.g., predicate pushdown).
type Stats struct {
	Min, Max uint64 // Using uint64 to generically represent min/max for numeric types
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
}

// Encode writes the block to the given writer.
func (b *Block) Encode(w io.Writer) error {
	// Write header
	if err := binary.Write(w, binary.LittleEndian, &b.Header); err != nil {
		return fmt.Errorf("failed to write block header: %w", err)
	}
	// Write stats
	if err := binary.Write(w, binary.LittleEndian, &b.Stats); err != nil {
		return fmt.Errorf("failed to write block stats: %w", err)
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
	// Read stats
	if err := binary.Read(r, binary.LittleEndian, &b.Stats); err != nil {
		return fmt.Errorf("failed to read block stats: %w", err)
	}
	// Read data
	b.Data = make([]byte, b.Header.StoredSizeBytes)
	_, err := io.ReadFull(r, b.Data)
	if err != nil {
		return fmt.Errorf("failed to read block data: %w", err)
	}
	return nil
}
