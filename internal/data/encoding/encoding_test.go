package encoding

import (
	"bytes"
	"testing"
)

const numValues = 1_000_000

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
