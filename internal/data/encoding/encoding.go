package encoding

import "io"

// Encoder is the interface for encoding a slice of values into a writer.
type Encoder interface {
	Encode(w io.Writer, src interface{}) error
}

// Decoder is the interface for decoding a slice of values from a reader.
type Decoder interface {
	Decode(r io.Reader, dst interface{}, numValues int) error
}
