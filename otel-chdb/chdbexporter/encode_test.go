package chdbexporter

import (
	"math"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// valueString must agree with pcommon.Value.AsString, which is what the
// clickhouse exporter stores, including on the paths it shortcuts.
func TestValueStringMatchesAsString(t *testing.T) {
	vals := []pcommon.Value{
		pcommon.NewValueStr("x\x00y"), pcommon.NewValueInt(-42), pcommon.NewValueBool(true),
		pcommon.NewValueDouble(0), pcommon.NewValueDouble(-0.0), pcommon.NewValueDouble(1.25),
		pcommon.NewValueDouble(1e-7), pcommon.NewValueDouble(1e-6), pcommon.NewValueDouble(123456789.125),
		pcommon.NewValueDouble(1e21), pcommon.NewValueDouble(9.99e20), pcommon.NewValueDouble(math.NaN()),
		pcommon.NewValueDouble(math.Inf(-1)), pcommon.NewValueEmpty(),
	}
	m := pcommon.NewValueMap()
	m.Map().PutStr("k", "v")
	s := pcommon.NewValueSlice()
	s.Slice().AppendEmpty().SetInt(1)
	by := pcommon.NewValueBytes()
	by.Bytes().FromRaw([]byte{0, 1, 2})
	vals = append(vals, m, s, by)
	for _, v := range vals {
		if got, want := string(valueString(nil, v)), v.AsString(); got != want {
			t.Errorf("%s: got %q, want %q", v.Type(), got, want)
		}
	}
}
