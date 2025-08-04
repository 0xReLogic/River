package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
)

// String is an encoder/decoder for string data.
// It stores offsets first, then the concatenated string data.
type String struct{}

// NewString creates a new String encoder/decoder.
func NewString() *String {
	return &String{}
}

// Encode writes a slice of strings to the writer.
func (e *String) Encode(w io.Writer, src interface{}) error {
	values, ok := src.([]string)
	if !ok {
		return fmt.Errorf("unsupported type for string encoding: %T", src)
	}

	offsets := make([]uint32, len(values))
	var totalSize uint32
	for i, v := range values {
		totalSize += uint32(len(v))
		offsets[i] = totalSize
	}

	// Write offsets
	if err := binary.Write(w, binary.LittleEndian, offsets); err != nil {
		return fmt.Errorf("failed to write string offsets: %w", err)
	}

	// Write data
	for _, v := range values {
		if _, err := io.WriteString(w, v); err != nil {
			return fmt.Errorf("failed to write string data: %w", err)
		}
	}

	return nil
}

// Decode reads a slice of strings from the reader.
func (e *String) Decode(r io.Reader, dst interface{}, numValues int) error {
	dstSlice, ok := dst.(*[]string)
	if !ok {
		return fmt.Errorf("unsupported type for string decoding: %T", dst)
	}

	if numValues == 0 {
		*dstSlice = []string{}
		return nil
	}

	offsets := make([]uint32, numValues)
	if err := binary.Read(r, binary.LittleEndian, offsets); err != nil {
		return fmt.Errorf("failed to read string offsets: %w", err)
	}

	totalSize := offsets[len(offsets)-1]
	data := make([]byte, totalSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return fmt.Errorf("failed to read string data: %w", err)
	}

	values := make([]string, numValues)
	var currentOffset uint32
	for i, offset := range offsets {
		values[i] = string(data[currentOffset:offset])
		currentOffset = offset
	}

	*dstSlice = values
	return nil
}
