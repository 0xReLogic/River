package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Fixed is an encoder/decoder for fixed-width data types.
// It uses binary.Write and binary.Read for efficient serialization.
type Fixed struct{}

// NewFixed creates a new Fixed encoder/decoder.
func NewFixed() *Fixed {
	return &Fixed{}
}

// Encode writes a slice of fixed-width values to the writer.
func (e *Fixed) Encode(w io.Writer, src interface{}) error {
	switch v := src.(type) {
	case []int32, []int64, []float32, []float64, []bool:
		return binary.Write(w, binary.LittleEndian, v)
	default:
		return fmt.Errorf("unsupported type for fixed encoding: %T", src)
	}
}

// Decode reads a slice of fixed-width values from the reader.
func (e *Fixed) Decode(r io.Reader, dst interface{}, numValues int) error {
	switch v := dst.(type) {
	case *[]int32:
		*v = make([]int32, numValues)
		return binary.Read(r, binary.LittleEndian, *v)
	case *[]int64:
		*v = make([]int64, numValues)
		return binary.Read(r, binary.LittleEndian, *v)
	case *[]float32:
		*v = make([]float32, numValues)
		return binary.Read(r, binary.LittleEndian, *v)
	case *[]float64:
		*v = make([]float64, numValues)
		return binary.Read(r, binary.LittleEndian, *v)
	case *[]bool:
		*v = make([]bool, numValues)
		return binary.Read(r, binary.LittleEndian, *v)
	default:
		return fmt.Errorf("unsupported type for fixed decoding: %T", dst)
	}
}
