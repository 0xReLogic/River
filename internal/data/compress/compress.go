package compress

// Compressor defines the interface for compressing and decompressing byte slices.
type Compressor interface {
	// Compress compresses the source byte slice and returns the compressed data.
	Compress(src []byte) ([]byte, error)

	// Decompress decompresses the source byte slice and returns the original data.
	Decompress(src []byte) ([]byte, error)
}
