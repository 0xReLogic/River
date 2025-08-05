package bitmap

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
)

// ToBytes serializes a roaring bitmap to a byte slice.
func ToBytes(bm *roaring.Bitmap) ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := bm.WriteTo(buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// FromBytes deserializes a roaring bitmap from a byte slice.
func FromBytes(b []byte) (*roaring.Bitmap, error) {
	bm := roaring.New()
	_, err := bm.ReadFrom(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return bm, nil
}
