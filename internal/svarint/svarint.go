package svarint

import (
	"golang.org/x/exp/constraints"
	"math/bits"
)

func Append[T constraints.Integer](buf []byte, x T) []byte {
	xl := 64 - bits.LeadingZeros64(uint64(x))
	switch {
	case xl <= 7:
		return append(buf, byte(x))
	case xl <= 14:
		return append(buf, byte(x>>7)|0x80, byte(x)&^0x80)
	case xl <= 21:
		return append(buf, byte(x>>14)|0x80, byte(x>>7)|0x80, byte(x)&^0x80)
	case xl <= 28:
		return append(buf, byte(x>>21)|0x80, byte(x>>14)|0x80, byte(x>>7)|0x80, byte(x)&^0x80)
	case xl <= 35:
		return append(buf, byte(x>>28)|0x80, byte(x>>21)|0x80, byte(x>>14)|0x80, byte(x>>7)|0x80, byte(x)&^0x80)
	case xl <= 42:
		return append(buf, byte(x>>35)|0x80, byte(x>>28)|0x80, byte(x>>21)|0x80, byte(x>>14)|0x80, byte(x>>7)|0x80, byte(x)&^0x80)
	case xl <= 49:
		return append(buf, byte(x>>42)|0x80, byte(x>>35)|0x80, byte(x>>28)|0x80, byte(x>>21)|0x80, byte(x>>14)|0x80, byte(x>>7)|0x80, byte(x)&^0x80)
	case xl <= 56:
		return append(buf, byte(x>>49)|0x80, byte(x>>42)|0x80, byte(x>>35)|0x80, byte(x>>28)|0x80, byte(x>>21)|0x80, byte(x>>14)|0x80, byte(x>>7)|0x80, byte(x)&^0x80)
	default:
		return append(buf, byte(x>>57)|0x80, byte(x>>50)|0x80, byte(x>>43)|0x80, byte(x>>36)|0x80, byte(x>>29)|0x80, byte(x>>22)|0x80, byte(x>>15)|0x80, byte(x>>8)|0x80, byte(x))
	}
}

func Length[T constraints.Integer](x T) int {
	xl := 64 - bits.LeadingZeros64(uint64(x))
	switch {
	case xl <= 7:
		return 1
	case xl <= 14:
		return 2
	case xl <= 21:
		return 3
	case xl <= 28:
		return 4
	case xl <= 35:
		return 5
	case xl <= 42:
		return 6
	case xl <= 49:
		return 7
	case xl <= 56:
		return 8
	default:
		return 9
	}
}

func Put[T constraints.Integer](buf []byte, x T) {
	xl := 64 - bits.LeadingZeros64(uint64(x))
	switch {
	case xl <= 7:
		buf[0] = byte(x)
	case xl <= 14:
		buf[0] = byte(x>>7) | 0x80
		buf[1] = byte(x) &^ 0x80
	case xl <= 21:
		buf[0] = byte(x>>14) | 0x80
		buf[1] = byte(x>>7) | 0x80
		buf[2] = byte(x) &^ 0x80
	case xl <= 28:
		buf[0] = byte(x>>21) | 0x80
		buf[1] = byte(x>>14) | 0x80
		buf[2] = byte(x>>7) | 0x80
		buf[3] = byte(x) &^ 0x80
	case xl <= 35:
		buf[0] = byte(x>>28) | 0x80
		buf[1] = byte(x>>21) | 0x80
		buf[2] = byte(x>>14) | 0x80
		buf[3] = byte(x>>7) | 0x80
		buf[4] = byte(x) &^ 0x80
	case xl <= 42:
		buf[0] = byte(x>>35) | 0x80
		buf[1] = byte(x>>28) | 0x80
		buf[2] = byte(x>>21) | 0x80
		buf[3] = byte(x>>14) | 0x80
		buf[4] = byte(x>>7) | 0x80
		buf[5] = byte(x) &^ 0x80
	case xl <= 49:
		buf[0] = byte(x>>42) | 0x80
		buf[1] = byte(x>>35) | 0x80
		buf[2] = byte(x>>28) | 0x80
		buf[3] = byte(x>>21) | 0x80
		buf[4] = byte(x>>14) | 0x80
		buf[5] = byte(x>>7) | 0x80
		buf[6] = byte(x) &^ 0x80
	case xl <= 56:
		buf[0] = byte(x>>49) | 0x80
		buf[1] = byte(x>>42) | 0x80
		buf[2] = byte(x>>35) | 0x80
		buf[3] = byte(x>>28) | 0x80
		buf[4] = byte(x>>21) | 0x80
		buf[5] = byte(x>>14) | 0x80
		buf[6] = byte(x>>7) | 0x80
		buf[7] = byte(x) &^ 0x80
	default:
		buf[0] = byte(x>>57) | 0x80
		buf[1] = byte(x>>50) | 0x80
		buf[2] = byte(x>>43) | 0x80
		buf[3] = byte(x>>36) | 0x80
		buf[4] = byte(x>>29) | 0x80
		buf[5] = byte(x>>22) | 0x80
		buf[6] = byte(x>>15) | 0x80
		buf[7] = byte(x>>8) | 0x80
		buf[8] = byte(x)
	}
}
