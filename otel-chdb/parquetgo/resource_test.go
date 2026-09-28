package parquetgo

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The shared resource_id vectors (../entities/testdata/resource_id_vectors.json,
// generated from the entity controller's internal/rid and checked by otap-rs
// tests/resource_id.rs): the Go edge's split and hash agree id for id.
func TestResourceIDVectors(t *testing.T) {
	b, err := os.ReadFile("../entities/testdata/resource_id_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		CoveredKeys []string `json:"covered_keys"`
		LabelPrefix string   `json:"label_prefix"`
		Vectors     []struct {
			Name  string `json:"name"`
			Attrs []struct {
				K    string `json:"k"`
				KHex string `json:"k_hex"`
				V    string `json:"v"`
				VHex string `json:"v_hex"`
				T    string `json:"t"`
			} `json:"attrs"`
			CoveredHex [][2]string `json:"covered_hex"`
			ResourceID string      `json:"resource_id"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.CoveredKeys, CoveredKeys) || f.LabelPrefix != LabelPrefix {
		t.Fatalf("covered keys differ from the vectors'")
	}
	unhex := func(s string) string {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if len(f.Vectors) < 15 {
		t.Fatalf("%d vectors", len(f.Vectors))
	}
	for _, v := range f.Vectors {
		kvs := make([]ResourceKV, len(v.Attrs))
		for i, a := range v.Attrs {
			k, val := a.K, a.V
			if a.KHex != "" {
				k = unhex(a.KHex)
			}
			if a.VHex != "" {
				val = unhex(a.VHex)
			}
			kvs[i] = ResourceKV{Key: k, Value: val, Str: a.T == "" || a.T == "str"}
		}
		c := SplitCovered(kvs)
		var got [][2]string
		for _, p := range c.Pairs {
			got = append(got, [2]string{hex.EncodeToString([]byte(p[0])), hex.EncodeToString([]byte(p[1]))})
		}
		if fmt.Sprint(got) != fmt.Sprint(v.CoveredHex) && !(len(got) == 0 && len(v.CoveredHex) == 0) {
			t.Errorf("%s: covered %v, want %v", v.Name, got, v.CoveredHex)
		}
		if id := strconv.FormatUint(c.ID, 10); id != v.ResourceID {
			t.Errorf("%s: resource_id %s, want %s", v.Name, id, v.ResourceID)
		}
		// the same through pdata, when it can hold the list (no duplicate keys)
		m := pcommon.NewMap()
		dup := false
		for _, kv := range kvs {
			if _, ok := m.Get(kv.Key); ok {
				dup = true
				break
			}
			if kv.Str {
				m.PutStr(kv.Key, kv.Value)
			} else {
				m.PutInt(kv.Key, 1)
			}
		}
		if !dup && CoveredOf(m).ID != c.ID {
			t.Errorf("%s: CoveredOf differs", v.Name)
		}
	}
}

func TestAnnounceCache(t *testing.T) {
	var c AnnounceCache
	if !c.Wants("e1", 1, 10) {
		t.Fatal("empty cache")
	}
	c.Announced("e1", []uint64{1, 2}, 10, 100)
	if c.Wants("e1", 1, 10) || !c.Wants("e1", 1, 11) || !c.Wants("e2", 1, 10) {
		t.Fatal("window / epoch")
	}
	c.Announced("e2", []uint64{3}, 10, 100)
	if c.Len() != 1 {
		t.Fatal("a new epoch empties the cache")
	}
	var ids []uint64
	for i := uint64(10); i < 30; i++ {
		ids = append(ids, i)
	}
	c.Announced("e2", ids, 10, 16)
	if c.Len() > 16 || c.Evicted == 0 || !c.Wants("e2", 3, 10) || c.Wants("e2", 29, 10) {
		t.Fatalf("eviction: len %d evicted %d", c.Len(), c.Evicted)
	}
}
