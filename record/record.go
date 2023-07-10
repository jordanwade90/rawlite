package record

import (
	"encoding/binary"
	"encoding/json"
	"github.com/jordanwade90/rawlite/internal/svarint"
	"math"
)

func headerLen(l int) int {
	return l + headerLenLen(l)
}

func headerLenLen(l int) int {
	// This unpleasant arithmetic is to handle the case where the header length varint
	// is long enough that it has to be extended a byte to hold the real length.
	return svarint.Length(l + svarint.Length(l))
}

type Record struct {
	header  []byte
	payload []byte
	offset  int
}

func (record *Record) AppendBlob(b []byte) {
	record.header = svarint.Append(record.header, 2*len(b)+12)
	record.payload = append(record.payload, b...)
}

func (record *Record) AppendBool(b bool) {
	if b {
		record.header = append(record.header, 9)
	} else {
		record.header = append(record.header, 8)
	}
}

func (record *Record) AppendFloat(f float64) {
	if i := int64(f); f == float64(i) {
		record.AppendInt(i)
	} else {
		record.header = append(record.header, 7)
		record.payload = binary.BigEndian.AppendUint64(record.payload, math.Float64bits(f))
	}
}

func (record *Record) AppendInt(i int64) {
	switch {
	case i == 0:
		record.header = append(record.header, 8)
	case i == 1:
		record.header = append(record.header, 9)
	case i >= -0x80 && i <= 0x7f:
		record.header = append(record.header, 1)
		record.payload = append(record.payload, byte(i))
	case i >= -0x8000 && i <= 0x7fff:
		record.header = append(record.header, 2)
		record.payload = append(record.payload, byte(i>>8), byte(i))
	case i >= -0x80_0000 && i <= 0x7f_ffff:
		record.header = append(record.header, 3)
		record.payload = append(record.payload, byte(i>>16), byte(i>>8), byte(i))
	case i >= -0x8000_0000 && i <= 0x7fff_ffff:
		record.header = append(record.header, 4)
		record.payload = append(record.payload, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i >= -0x8000_0000_0000 && i <= 0x7fff_ffff_ffff:
		record.header = append(record.header, 5)
		record.payload = append(record.payload, byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	default:
		record.header = append(record.header, 6)
		record.payload = append(record.payload, byte(i>>56), byte(i>>48), byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	}
}

func (record *Record) AppendNull() {
	record.header = append(record.header, 0)
}

func (record *Record) AppendJSON(v any) error {
	s, err := json.Marshal(v)
	if err != nil {
		return err
	}

	record.header = svarint.Append(record.header, 2*len(s)+13)
	record.payload = append(record.payload, s...)
	return nil
}

func (record *Record) AppendString(s string) {
	record.header = svarint.Append(record.header, 2*len(s)+13)
	record.payload = append(record.payload, s...)
}

func (record *Record) AppendStringSlice(s []byte) {
	record.header = svarint.Append(record.header, 2*len(s)+13)
	record.payload = append(record.payload, s...)
}

func (record *Record) AppendUint(i uint64) {
	switch {
	case i == 0:
		record.header = append(record.header, 8)
	case i == 1:
		record.header = append(record.header, 9)
	case i <= 0x7f:
		record.header = append(record.header, 1)
		record.payload = append(record.payload, byte(i))
	case i <= 0x7fff:
		record.header = append(record.header, 2)
		record.payload = append(record.payload, byte(i>>8), byte(i))
	case i <= 0x7f_ffff:
		record.header = append(record.header, 3)
		record.payload = append(record.payload, byte(i>>16), byte(i>>8), byte(i))
	case i <= 0x7fff_ffff:
		record.header = append(record.header, 4)
		record.payload = append(record.payload, byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	case i <= 0x7fff_ffff_ffff:
		record.header = append(record.header, 5)
		record.payload = append(record.payload, byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	default:
		record.header = append(record.header, 6)
		record.payload = append(record.payload, byte(i>>56), byte(i>>48), byte(i>>40), byte(i>>32), byte(i>>24), byte(i>>16), byte(i>>8), byte(i))
	}
}

func (record *Record) AppendTo(p []byte) []byte {
	p = svarint.Append(p, headerLen(len(record.header)))
	p = append(p, record.header...)
	p = append(p, record.payload...)
	return p
}

func (record *Record) Reset() {
	record.header = record.header[:0]
	record.payload = record.payload[:0]
	record.offset = 0
}
