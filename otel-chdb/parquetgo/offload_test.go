package parquetgo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/zeebo/blake3"
)

// The shared vectors (../langfuse/testdata/offload_vectors.json), written
// by otap-rs tests/offload.rs: the Go offloader must give the Rust edge's
// hashes, splits, cuts, stored values, markers, references and payloads,
// byte for byte (FORMAT.md §2.3).

type vecValue struct {
	Runs [][2]json.RawMessage `json:"runs"`
}

func (v vecValue) bytes(t *testing.T) []byte {
	var out []byte
	for _, r := range v.Runs {
		var hx string
		var n int
		if err := json.Unmarshal(r[0], &hx); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(r[1], &n); err != nil {
			t.Fatal(err)
		}
		b, err := hex.DecodeString(hx)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, bytes.Repeat(b, n)...)
	}
	return out
}

type vectors struct {
	HashContext string `json:"hash_context"`
	Hashes      []struct {
		Cluster      string `json:"cluster"`
		NamespaceHex string `json:"namespace_hex"`
		ReceivedNs   uint64 `json:"received_ns"`
		ContentHex   string `json:"content_hex"`
		Key          string `json:"key"`
		Hash         string `json:"hash"`
	} `json:"hashes"`
	Splits []struct {
		Value       vecValue `json:"value"`
		MaxDepth    int      `json:"max_depth"`
		MaxElements int      `json:"max_elements"`
		Elements    *struct {
			N      int       `json:"n"`
			Ranges *[][2]int `json:"ranges"`
			Digest string    `json:"digest"`
		} `json:"elements"`
	} `json:"splits"`
	Truncations []struct {
		ValueHex string   `json:"value_hex"`
		Cuts     [][2]int `json:"cuts"`
	} `json:"truncations"`
	Requests []struct {
		Name       string         `json:"name"`
		Policy     OffloadOptions `json:"policy"`
		Cluster    string         `json:"cluster"`
		Namespace  string         `json:"namespace"`
		ReceivedNs uint64         `json:"received_ns"`
		Values     []struct {
			KeyHex string   `json:"key_hex"`
			Value  vecValue `json:"value"`
		} `json:"values"`
		Want struct {
			Values []struct {
				Stored  *string     `json:"stored"`
				Markers [][2]string `json:"markers"`
				RowRefs []string    `json:"row_refs"`
			} `json:"values"`
			Payloads []struct {
				Hash     string `json:"hash"`
				Len      int    `json:"len"`
				Blake3   string `json:"blake3"`
				FirstRow uint32 `json:"first_row"`
			} `json:"payloads"`
			Stats map[string]uint64 `json:"stats"`
		} `json:"want"`
	} `json:"requests"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	b, err := os.ReadFile("../langfuse/testdata/offload_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if v.HashContext != PayloadHashContext {
		t.Fatalf("hash context %q, want %q", v.HashContext, PayloadHashContext)
	}
	return v
}

func unhex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestOffloadVectorsHashes(t *testing.T) {
	v := loadVectors(t)
	if len(v.Hashes) == 0 {
		t.Fatal("no hash vectors")
	}
	for i, h := range v.Hashes {
		k := TenantKey(h.Cluster, unhex(t, h.NamespaceHex), h.ReceivedNs)
		if got := hex.EncodeToString(k[:]); got != h.Key {
			t.Errorf("hash %d: key %s, want %s", i, got, h.Key)
		}
		p := PayloadHash(&k, unhex(t, h.ContentHex))
		if got := hex.EncodeToString(p[:]); got != h.Hash {
			t.Errorf("hash %d: %s, want %s", i, got, h.Hash)
		}
	}
}

func TestOffloadVectorsSplits(t *testing.T) {
	v := loadVectors(t)
	for i, s := range v.Splits {
		b := s.Value.bytes(t)
		got, ok := JSONArrayElements(b, s.MaxDepth, s.MaxElements)
		if s.Elements == nil {
			if ok {
				t.Errorf("split %d (%.40q): split into %d, want not an array", i, b, len(got))
			}
			continue
		}
		if !ok {
			t.Errorf("split %d (%.40q): not split, want %d elements", i, b, s.Elements.N)
			continue
		}
		h := blake3.New()
		for _, r := range got {
			_, _ = h.Write(binary.LittleEndian.AppendUint64(nil, uint64(r[0])))
			_, _ = h.Write(binary.LittleEndian.AppendUint64(nil, uint64(r[1])))
		}
		if len(got) != s.Elements.N || hex.EncodeToString(h.Sum(nil)) != s.Elements.Digest {
			t.Errorf("split %d (%.40q): %d elements %v, want %d (digest)", i, b, len(got), got, s.Elements.N)
		}
		if s.Elements.Ranges != nil && len(*s.Elements.Ranges) != len(got) {
			t.Errorf("split %d: ranges", i)
		}
	}
}

func TestOffloadVectorsTruncations(t *testing.T) {
	v := loadVectors(t)
	for _, tr := range v.Truncations {
		b := unhex(t, tr.ValueHex)
		for _, c := range tr.Cuts {
			if got := TruncateAt(b, c[0]); got != c[1] {
				t.Errorf("%x cut at %d: %d, want %d", b, c[0], got, c[1])
			}
		}
	}
}

func TestOffloadVectorsRequests(t *testing.T) {
	v := loadVectors(t)
	if len(v.Requests) == 0 {
		t.Fatal("no request vectors")
	}
	for _, r := range v.Requests {
		t.Run(r.Name, func(t *testing.T) {
			if err := r.Policy.Validate(); err != nil {
				t.Fatal(err)
			}
			o := NewOffloader(NewOffloadPolicy(r.Policy), r.Cluster, r.ReceivedNs)
			o.Tenant(r.Namespace)
			for i, val := range r.Values {
				key := string(unhex(t, val.KeyHex))
				out, replaced := o.Value(key, val.Value.bytes(t), uint32(i))
				w := r.Want.Values[i]
				switch {
				case w.Stored == nil && replaced:
					t.Errorf("value %d (%q): replaced by %.60s, want inline", i, key, out)
				case w.Stored != nil && !replaced:
					t.Errorf("value %d (%q): inline, want %.60s", i, key, *w.Stored)
				case w.Stored != nil && string(out) != *w.Stored:
					t.Errorf("value %d (%q): %.80s, want %.80s", i, key, out, *w.Stored)
				}
				var ms [][2]string
				for _, m := range o.Markers {
					ms = append(ms, [2]string{hex.EncodeToString([]byte(m[0])), m[1]})
				}
				o.Markers = o.Markers[:0]
				if !equalPairs(ms, w.Markers) {
					t.Errorf("value %d (%q): markers %v, want %v", i, key, ms, w.Markers)
				}
				var refs []string
				for _, h := range o.EndRow() {
					refs = append(refs, hex.EncodeToString(h[:]))
				}
				if strings.Join(refs, ",") != strings.Join(w.RowRefs, ",") {
					t.Errorf("value %d (%q): refs %v, want %v", i, key, refs, w.RowRefs)
				}
			}
			if len(o.Payloads) != len(r.Want.Payloads) {
				t.Fatalf("%d payloads, want %d", len(o.Payloads), len(r.Want.Payloads))
			}
			for i, p := range o.Payloads {
				w := r.Want.Payloads[i]
				sum := blake3.Sum256(p.Content)
				if hex.EncodeToString(p.Hash[:]) != w.Hash || len(p.Content) != w.Len || hex.EncodeToString(sum[:]) != w.Blake3 || p.FirstRow != w.FirstRow {
					t.Errorf("payload %d: %x len %d row %d, want %s len %d row %d", i, p.Hash, len(p.Content), p.FirstRow, w.Hash, w.Len, w.FirstRow)
				}
			}
			got := map[string]uint64{"offloaded": o.Stats.Offloaded, "offloaded_bytes": o.Stats.OffloadedBytes,
				"split": o.Stats.Split, "truncated": o.Stats.Truncated, "redacted": o.Stats.Redacted}
			for k, want := range r.Want.Stats {
				if got[k] != want {
					t.Errorf("stats %s: %d, want %d", k, got[k], want)
				}
			}
		})
	}
}

func equalPairs(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestOffloadOptionsValidated(t *testing.T) {
	if err := DefaultOffloadOptions().Validate(); err != nil {
		t.Fatal(err)
	}
	off := DefaultOffloadOptions()
	off.Enabled, off.Threshold = false, 0
	if err := off.Validate(); err != nil {
		t.Fatal(err)
	}
	for i, f := range []func(*OffloadOptions){
		func(o *OffloadOptions) { o.Threshold = 8 << 20 },
		func(o *OffloadOptions) { o.MaxValue = 256 << 20 },
		func(o *OffloadOptions) { o.MaxRequestBytes, o.MaxValue = 2<<30, 2<<30 },
		func(o *OffloadOptions) { o.SplitMaxDepth = 0 },
		func(o *OffloadOptions) { o.SplitMaxElements = 0 },
		func(o *OffloadOptions) { o.CacheSize = 0 },
		func(o *OffloadOptions) { o.SplitKeys = []string{"not.listed"} },
		func(o *OffloadOptions) { o.Keys, o.SplitKeys = []string{"otel.payload.x"}, nil },
	} {
		o := DefaultOffloadOptions()
		f(&o)
		if o.Validate() == nil {
			t.Errorf("case %d: %+v validated", i, o)
		}
	}
}

func TestPayloadCachePerEpochAndBounded(t *testing.T) {
	var c PayloadCache
	h := func(i byte) [16]byte { return [16]byte{i} }
	if !c.Wants("e1", h(1)) {
		t.Fatal("empty cache")
	}
	c.Sent("e1", [][16]byte{h(1), h(2)}, 100)
	if c.Wants("e1", h(1)) || !c.Wants("e2", h(1)) {
		t.Fatal("per epoch")
	}
	c.Sent("e2", [][16]byte{h(3)}, 100)
	if c.Len() != 1 {
		t.Fatal("a new epoch starts empty")
	}
	var many [][16]byte
	for i := 10; i < 30; i++ {
		many = append(many, h(byte(i)))
	}
	c.Sent("e2", many, 16)
	if c.Len() > 16 || c.Evicted == 0 || !c.Wants("e2", h(3)) {
		t.Fatalf("bounded: len %d evicted %d", c.Len(), c.Evicted)
	}
}

func TestJSONScanIsBounded(t *testing.T) {
	deep := strings.Repeat("[", 100_000) + strings.Repeat("]", 100_000)
	if _, ok := JSONArrayElements([]byte(deep), 64, 4096); ok {
		t.Fatal("a bomb deeper than the bound is split")
	}
	many := "[" + strings.TrimSuffix(strings.Repeat("1,", 10_000), ",") + "]"
	if _, ok := JSONArrayElements([]byte(many), 64, 4096); ok {
		t.Fatal("more elements than the bound")
	}
	if e, ok := JSONArrayElements([]byte(many), 64, 10_000); !ok || len(e) != 10_000 {
		t.Fatal(strconv.Itoa(len(e)))
	}
}
