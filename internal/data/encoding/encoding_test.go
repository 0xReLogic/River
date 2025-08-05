package encoding

import (
	"bytes"
	"testing"
)

const numValues = 1_000_000

func TestFixedEncodeDecodeInt64(t *testing.T) {
	encoder := NewFixed()
	values := make([]int64, 100)
	for i := 0; i < 100; i++ {
		values[i] = int64(i)
	}

	// Encode
	buf := new(bytes.Buffer)
	err := encoder.Encode(buf, values)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Decode
	decodedValues := make([]int64, 0)
	reader := bytes.NewReader(buf.Bytes())
	err = encoder.Decode(reader, &decodedValues, 100)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Verify
	if len(decodedValues) != 100 {
		t.Errorf("Expected 100 values, got %d", len(decodedValues))
	}
	for i := 0; i < 100; i++ {
		if decodedValues[i] != int64(i) {
			t.Errorf("Value mismatch at index %d: expected %d, got %d", i, i, decodedValues[i])
		}
	}
}

func BenchmarkFixedEncode_Int64(b *testing.B) {
	encoder := NewFixed()
	values := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int64(i)
	}

	buf := new(bytes.Buffer)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := encoder.Encode(buf, values)
		if err != nil {
			b.Fatal(err)
		}
	}

	// Report the size of the encoded data for 1M int64 values.
	// 1M * 8 bytes/int64 = 8MB. This should be well under the 250MB target.
	b.SetBytes(int64(buf.Len()))
}

func BenchmarkFixedDecode_Int64(b *testing.B) {
	encoder := NewFixed()
	values := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int64(i)
	}

	buf := new(bytes.Buffer)
	err := encoder.Encode(buf, values)
	if err != nil {
		b.Fatal(err)
	}
	encodedBytes := buf.Bytes()

	decodedValues := make([]int64, numValues)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(encodedBytes)
		err := encoder.Decode(reader, &decodedValues, numValues)
		if err != nil {
			b.Fatal(err)
		}
	}
}
