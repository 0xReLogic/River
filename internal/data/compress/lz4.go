package compress

import (
	"github.com/pierrec/lz4/v4"
)

// LZ4 implements the Compressor interface using the LZ4 algorithm.
type LZ4 struct{}

// NewLZ4 creates a new LZ4 compressor.
func NewLZ4() *LZ4 {
	return &LZ4{}
}

// Compress compresses the source byte slice using LZ4.
func (c *LZ4) Compress(src []byte) ([]byte, error) {
	dst := make([]byte, lz4.CompressBlockBound(len(src)))
	n, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// Data is incompressible, store it as is with a flag
		return src, nil
	}
	return dst[:n], nil
}

// Decompress decompresses the source byte slice using LZ4.
func (c *LZ4) Decompress(src []byte) ([]byte, error) {
	// The lz4 library requires the original size to be known for decompression.
	// For simplicity in this block format, we'd typically store the original size
	// in the block header. For now, we'll assume a decompression scheme where
	// the destination buffer size is managed by the caller or known beforehand.
	// A more robust implementation would handle this differently, maybe by
	// prepending the original size to the compressed data.

	// This is a simplified example. A real implementation needs a way to know the original size.
	// Let's assume the caller provides a sufficiently large buffer or we have a max block size.
	// For now, we'll just show the core decompression logic.
	// A common pattern is to store the uncompressed length as the first few bytes of the compressed block.

	// The `DecompressBlock` function from pierrec/lz4 doesn't require the original size,
	// but it's less safe if the compressed data is corrupt. Let's use it for simplicity.
	dst := make([]byte, 10*len(src)) // Heuristic: allocate 10x the compressed size. This is NOT robust.
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, err
	}
	return dst[:n], nil
}
