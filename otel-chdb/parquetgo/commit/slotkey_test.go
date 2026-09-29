package commit

import (
	"testing"
)

// A slot has one key: ParseSlotKey accepts exactly what SlotKey writes, so
// a second spelling of a slot's key ("…/E1/7.parquet", "+7", a sign, no
// padding, 21 digits) is not a slot (the Rust consumer's parse_slot_key
// agrees; otap-rs a_second_spelling_of_a_slot_key_is_not_a_slot).
func TestParseSlotKeyAcceptsOnlyTheSlotsKey(t *testing.T) {
	const p = "r/c1/p1/traces"
	for _, seq := range []uint64{0, 7, 1<<64 - 1} {
		k := SlotKey(p, "E1", seq)
		if e, s, ok := ParseSlotKey(p, k); !ok || e != "E1" || s != seq {
			t.Errorf("%s: %q %d %v", k, e, s, ok)
		}
	}
	for _, k := range []string{p + "/E1/7.parquet", p + "/E1/+0000000000000000007.parquet", p + "/E1/000000000000000000007.parquet",
		p + "/E1/0000000000000000000x.parquet", p + "//00000000000000000007.parquet", p + "/E1/00000000000000000007.parquet/x",
		p + "/E1/00000000000000000007", "r/c1/p1/logs/E1/00000000000000000007.parquet", p + "/E1/99999999999999999999.parquet"} {
		if e, s, ok := ParseSlotKey(p, k); ok {
			t.Errorf("%s accepted as %q %d", k, e, s)
		}
	}
}

// FuzzParseSlotKey: an accepted key is SlotKey of what it parsed to.
func FuzzParseSlotKey(f *testing.F) {
	for _, k := range []string{"r/p/E1/00000000000000000007.parquet", "r/p/E1/7.parquet", "r/p/E1/+7.parquet", "r/p//x.parquet"} {
		f.Add("r/p", k)
	}
	f.Fuzz(func(t *testing.T, prefix, key string) {
		if e, s, ok := ParseSlotKey(prefix, key); ok && SlotKey(prefix, e, s) != key {
			t.Fatalf("ParseSlotKey(%q, %q) = %q, %d: SlotKey gives %q", prefix, key, e, s, SlotKey(prefix, e, s))
		}
	})
}
