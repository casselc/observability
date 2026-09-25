package otap

import (
	"encoding/binary"
	"errors"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// decodeCBOR decodes an OTAP `ser` value (a CBOR-encoded map or slice) into
// v, keeping map keys in their encoded order.
//
// The Go library's common.Deserialize decodes into map[interface{}]interface{},
// so it returns nested map keys in random order, and the JSON that
// Value.AsString renders from it changes from run to run
// (TestLibraryCBORMapOrder). This small decoder covers the subset the
// producer emits (common/cbor.go encode): ints, floats, text, bytes, bool,
// null, and definite or indefinite arrays and maps.
func decodeCBOR(b []byte, v pcommon.Value) error {
	d := cborDec{b: b}
	if err := d.value(v); err != nil {
		return err
	}
	return nil
}

type cborDec struct {
	b []byte
	i int
}

var errCBOR = errors.New("otap: malformed CBOR")

func (d *cborDec) head() (major byte, ai byte, arg uint64, err error) {
	if d.i >= len(d.b) {
		return 0, 0, 0, errCBOR
	}
	c := d.b[d.i]
	d.i++
	major, ai = c>>5, c&31
	switch {
	case ai < 24:
		arg = uint64(ai)
	case ai == 24:
		if d.i+1 > len(d.b) {
			return 0, 0, 0, errCBOR
		}
		arg = uint64(d.b[d.i])
		d.i++
	case ai == 25:
		if d.i+2 > len(d.b) {
			return 0, 0, 0, errCBOR
		}
		arg = uint64(binary.BigEndian.Uint16(d.b[d.i:]))
		d.i += 2
	case ai == 26:
		if d.i+4 > len(d.b) {
			return 0, 0, 0, errCBOR
		}
		arg = uint64(binary.BigEndian.Uint32(d.b[d.i:]))
		d.i += 4
	case ai == 27:
		if d.i+8 > len(d.b) {
			return 0, 0, 0, errCBOR
		}
		arg = binary.BigEndian.Uint64(d.b[d.i:])
		d.i += 8
	case ai == 31:
	default:
		return 0, 0, 0, errCBOR
	}
	return major, ai, arg, nil
}

func (d *cborDec) isBreak() bool {
	if d.i < len(d.b) && d.b[d.i] == 0xff {
		d.i++
		return true
	}
	return false
}

func (d *cborDec) str(ai byte, arg uint64) ([]byte, error) {
	if ai != 31 {
		if uint64(len(d.b)-d.i) < arg {
			return nil, errCBOR
		}
		s := d.b[d.i : d.i+int(arg)]
		d.i += int(arg)
		return s, nil
	}
	var out []byte
	for !d.isBreak() {
		_, cai, carg, err := d.head()
		if err != nil {
			return nil, err
		}
		s, err := d.str(cai, carg)
		if err != nil {
			return nil, err
		}
		out = append(out, s...)
	}
	return out, nil
}

func (d *cborDec) value(v pcommon.Value) error {
	major, ai, arg, err := d.head()
	if err != nil {
		return err
	}
	switch major {
	case 0:
		if arg > math.MaxInt64 {
			return errCBOR
		}
		v.SetInt(int64(arg))
	case 1:
		v.SetInt(-1 - int64(arg))
	case 2:
		s, err := d.str(ai, arg)
		if err != nil {
			return err
		}
		v.SetEmptyBytes().FromRaw(s)
	case 3:
		s, err := d.str(ai, arg)
		if err != nil {
			return err
		}
		v.SetStr(string(s))
	case 4:
		sl := v.SetEmptySlice()
		for n := uint64(0); ai == 31 || n < arg; n++ {
			if ai == 31 && d.isBreak() {
				break
			}
			if err := d.value(sl.AppendEmpty()); err != nil {
				return err
			}
		}
	case 5:
		m := v.SetEmptyMap()
		for n := uint64(0); ai == 31 || n < arg; n++ {
			if ai == 31 && d.isBreak() {
				break
			}
			kmaj, kai, karg, err := d.head()
			if err != nil || kmaj != 3 {
				return errCBOR
			}
			k, err := d.str(kai, karg)
			if err != nil {
				return err
			}
			if err := d.value(m.PutEmpty(string(k))); err != nil {
				return err
			}
		}
	case 6: // tag: decode the tagged item
		return d.value(v)
	case 7:
		switch ai {
		case 20:
			v.SetBool(false)
		case 21:
			v.SetBool(true)
		case 22, 23:
			// null/undefined: leave empty
		case 25:
			v.SetDouble(float64(half(uint16(arg))))
		case 26:
			v.SetDouble(float64(math.Float32frombits(uint32(arg))))
		case 27:
			v.SetDouble(math.Float64frombits(arg))
		default:
			return errCBOR
		}
	}
	return nil
}

// half converts an IEEE 754 half-precision float.
func half(h uint16) float32 {
	sign := uint32(h>>15) << 31
	exp := uint32(h>>10) & 0x1f
	frac := uint32(h & 0x3ff)
	switch {
	case exp == 0:
		f := float32(frac) / 1024 * float32(math.Pow(2, -14))
		if sign != 0 {
			return -f
		}
		return f
	case exp == 31:
		return math.Float32frombits(sign | 0x7f800000 | frac<<13)
	}
	return math.Float32frombits(sign | (exp+112)<<23 | frac<<13)
}
