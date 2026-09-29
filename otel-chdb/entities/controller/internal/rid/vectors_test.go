package rid

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

// The shared resource_id vectors (../../testdata/resource_id_vectors.json):
// attribute lists as an edge sees them -> the covered set and resource_id.
// This package is the reference; otap-rs (tests/resource_id.rs) and
// parquetgo (resource_test.go) read the same file. `go test ./internal/rid
// -run TestVectors -update` regenerates it from the cases below.
var update = flag.Bool("update", false, "rewrite the shared vectors from the cases")

const vectorsPath = "../../../testdata/resource_id_vectors.json"

// vAttr is one attribute in the file: a string value in v (or its bytes in
// v_hex when they are not UTF-8), the key likewise; t names a non-string type.
type vAttr struct {
	K    string `json:"k,omitempty"`
	KHex string `json:"k_hex,omitempty"`
	V    string `json:"v,omitempty"`
	VHex string `json:"v_hex,omitempty"`
	T    string `json:"t,omitempty"`
}

type vector struct {
	Name          string            `json:"name"`
	Attrs         []vAttr           `json:"attrs"`
	Covered       map[string]string `json:"covered,omitempty"`
	CoveredHex    [][2]string       `json:"covered_hex"`
	ResourceID    string            `json:"resource_id"`
	ResourceIDHex string            `json:"resource_id_hex"`
}

type vectorFile struct {
	Comment     []string `json:"comment"`
	CoveredKeys []string `json:"covered_keys"`
	LabelPrefix string   `json:"label_prefix"`
	Vectors     []vector `json:"vectors"`
}

func s(k, v string) vAttr { return enc(k, v, "") }

func enc(k, v, t string) vAttr {
	a := vAttr{T: t}
	if utf8.ValidString(k) {
		a.K = k
	} else {
		a.KHex = hex.EncodeToString([]byte(k))
	}
	if utf8.ValidString(v) {
		a.V = v
	} else {
		a.VHex = hex.EncodeToString([]byte(v))
	}
	return a
}

func (a vAttr) kv(t *testing.T) KV {
	k, v := a.K, a.V
	if a.KHex != "" {
		b, err := hex.DecodeString(a.KHex)
		if err != nil {
			t.Fatal(err)
		}
		k = string(b)
	}
	if a.VHex != "" {
		b, err := hex.DecodeString(a.VHex)
		if err != nil {
			t.Fatal(err)
		}
		v = string(b)
	}
	return KV{Key: k, Value: v, Str: a.T == "" || a.T == "str"}
}

func realistic() []vAttr {
	return []vAttr{
		s("k8s.cluster.name", "prod-use1-00"), s("k8s.cluster.uid", "0b7c1e2a-7d1f-4a51-9b1e-6c3f2d9e8a01"),
		s("cloud.provider", "aws"), s("cloud.platform", "aws_eks"), s("cloud.region", "us-east-1"),
		s("cloud.account.id", "123456789012"), s("cloud.availability.zone", "us-east-1a"),
		s("deployment.environment.name", "prod"),
		s("k8s.node.name", "ip-10-0-1-23.ec2.internal"), s("k8s.node.uid", "5f0e7d1c-0000-4000-8000-000000000001"),
		s("host.name", "ip-10-0-1-23.ec2.internal"), s("host.id", "i-0123456789abcdef0"), s("host.type", "m6i.2xlarge"),
		s("k8s.namespace.name", "checkout"),
		s("k8s.replicaset.name", "cart-7d9f8c6b5"), s("k8s.deployment.name", "cart"), s("service.name", "cart"),
		s("k8s.pod.name", "cart-7d9f8c6b5-x2x9z"), s("k8s.pod.uid", "9a1b2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d"),
		s("k8s.pod.start_time", "2026-09-28T10:11:12Z"),
		s("k8s.pod.label.app.kubernetes.io/name", "cart"), s("k8s.pod.label.pod-template-hash", "7d9f8c6b5"),
		s("k8s.pod.label.team", "payments"),
		s("k8s.container.name", "cart"), s("container.image.name", "registry.example.com/shop/cart"),
		s("container.image.tag", "1.42.0"),
		// the residual: SDK-set, never hashed
		s("telemetry.sdk.name", "opentelemetry"), s("telemetry.sdk.language", "go"), s("telemetry.sdk.version", "1.38.0"),
		s("service.instance.id", "cart-7d9f8c6b5-x2x9z/cart"), s("service.version", "1.42.0"),
		enc("process.pid", "1", "int"), s("process.runtime.name", "go"),
	}
}

func cases() []vector {
	rev := realistic()
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	many := []vAttr{s("k8s.pod.uid", "u-many"), s("k8s.namespace.name", "ns")}
	for i := 0; i < 300; i++ {
		many = append(many, s(fmt.Sprintf("k8s.pod.label.l%03d", 299-i), strings.Repeat("v", i%7)+strconv.Itoa(i)))
	}
	for i := 0; i < 50; i++ {
		many = append(many, s(fmt.Sprintf("custom.attr.%d", i), "x"))
	}
	long := strings.Repeat("λ", 4<<10) // 8 KiB of two-byte runes
	return []vector{
		{Name: "empty", Attrs: []vAttr{}},
		{Name: "clickhouse reference (sql/resources.sql over the same map)", Attrs: []vAttr{s("k8s.pod.name", "a-1"), s("k8s.namespace.name", "ns"), s("empty", "")}},
		{Name: "realistic pod container with an SDK residual", Attrs: realistic()},
		{Name: "the same, attributes in reverse order", Attrs: rev},
		{Name: "only keys outside the covered set", Attrs: []vAttr{s("telemetry.sdk.name", "x"), s("service.version", "1"), s("service.namespace", "shop"), s("custom", "y")}},
		{Name: "empty values are not covered", Attrs: []vAttr{s("k8s.pod.name", "p"), s("k8s.node.name", ""), s("k8s.pod.label.team", ""), s("k8s.namespace.name", "ns")}},
		{Name: "unicode values", Attrs: []vAttr{s("k8s.pod.name", "café-☕-🚀"), s("k8s.namespace.name", "日本語"), s("service.name", "e\u0301\u200d"), s("k8s.pod.label.note", "עברית \u202ertl")}},
		{Name: "unicode and odd label keys, bytewise order", Attrs: []vAttr{s("k8s.pod.label.é", "1"), s("k8s.pod.label.Z", "2"), s("k8s.pod.label.a", "3"), s("k8s.pod.label.a.b", "4"), s("k8s.pod.label.a b", "5"), s("k8s.pod.label.~", "6")}},
		{Name: "300 labels and 50 residual keys", Attrs: many},
		{Name: "duplicate key: the first occurrence decides", Attrs: []vAttr{s("k8s.pod.name", "first"), s("k8s.pod.name", "second"), s("k8s.node.name", ""), s("k8s.node.name", "later-node")}},
		{Name: "non-string values are not covered", Attrs: []vAttr{enc("k8s.pod.name", "42", "int"), enc("k8s.namespace.name", "true", "bool"), enc("host.id", "AQID", "bytes"), s("k8s.container.name", "c")}},
		{Name: "a NUL in a value is not covered", Attrs: []vAttr{s("k8s.pod.name", "a\x00k8s.pod.uid\x00b"), s("k8s.namespace.name", "ns")}},
		{Name: "near-miss keys", Attrs: []vAttr{s("K8s.pod.name", "a"), s("k8s.pod.name ", "b"), s("k8s.pod.labels.x", "c"), s("k8s.pod.label.", "d"), s("k8s.pod.label", "e"), s("service.namespace", "f"), s("k8s.pod.uid", "u")}},
		{Name: "invalid UTF-8 value is hashed as bytes", Attrs: []vAttr{s("k8s.pod.name", "p\xff\xfeq"), s("k8s.namespace.name", "ns")}},
		{Name: "invalid UTF-8 label key", Attrs: []vAttr{s("k8s.pod.label.\xc3(", "v"), s("k8s.namespace.name", "ns")}},
		{Name: "an 8 KiB value", Attrs: []vAttr{s("k8s.pod.name", long), s("k8s.namespace.name", "ns")}},
		{Name: "separator-like but separate keys", Attrs: []vAttr{s("k8s.pod.name", "a"), s("k8s.pod.uid", "b")}},
	}
}

func fill(t *testing.T, v *vector) {
	kvs := make([]KV, len(v.Attrs))
	for i, a := range v.Attrs {
		kvs[i] = a.kv(t)
	}
	cov := Split(kvs)
	keys := make([]string, 0, len(cov))
	allUTF8 := true
	for k, val := range cov {
		keys = append(keys, k)
		allUTF8 = allUTF8 && utf8.ValidString(k) && utf8.ValidString(val)
	}
	sort.Strings(keys)
	v.CoveredHex = [][2]string{}
	v.Covered = nil
	if allUTF8 && len(cov) > 0 && len(cov) <= 40 {
		v.Covered = cov
	}
	for _, k := range keys {
		v.CoveredHex = append(v.CoveredHex, [2]string{hex.EncodeToString([]byte(k)), hex.EncodeToString([]byte(cov[k]))})
	}
	id := ID(cov)
	v.ResourceID = strconv.FormatUint(id, 10)
	v.ResourceIDHex = fmt.Sprintf("%016x", id)
}

func TestVectors(t *testing.T) {
	if *update {
		f := vectorFile{
			Comment: []string{
				"resource_id test vectors, shared by the controller (internal/rid), otap-rs (tests/resource_id.rs) and parquetgo (resource_test.go).",
				"resource_id = xxh3_64(\"res.v1\\0\" ++ for (k, v) in covered, sorted by k bytewise: k ++ \"\\0\" ++ v ++ \"\\0\"), seed 0 (entities/README.md §3.1).",
				"covered: the FIRST occurrence of each key; kept if the key is in covered_keys or is label_prefix + a non-empty rest without NUL, and the value is a string, non-empty, without NUL.",
				"attrs: k (or k_hex), v (or v_hex for bytes that are not UTF-8), t = a non-string type (int, bool, bytes). covered_hex: [key hex, value hex], sorted. Regenerate: go test ./internal/rid -run TestVectors -update (entities/controller).",
			},
			CoveredKeys: CoveredKeys, LabelPrefix: LabelPrefix,
		}
		for _, v := range cases() {
			fill(t, &v)
			f.Vectors = append(f.Vectors, v)
		}
		var buf bytes.Buffer
		e := json.NewEncoder(&buf)
		e.SetEscapeHTML(false)
		e.SetIndent("", " ")
		if err := e.Encode(f); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vectorsPath, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	var f vectorFile
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	if strings.Join(f.CoveredKeys, ",") != strings.Join(CoveredKeys, ",") || f.LabelPrefix != LabelPrefix {
		t.Fatalf("the file's covered keys differ from rid.CoveredKeys: regenerate with -update")
	}
	if len(f.Vectors) < 15 {
		t.Fatalf("%d vectors", len(f.Vectors))
	}
	for _, want := range f.Vectors {
		got := want
		fill(t, &got)
		if got.ResourceID != want.ResourceID || fmt.Sprint(got.CoveredHex) != fmt.Sprint(want.CoveredHex) {
			t.Errorf("%s: got %s %v, want %s %v", want.Name, got.ResourceID, got.CoveredHex, want.ResourceID, want.CoveredHex)
		}
	}
	byName := map[string]string{}
	for _, v := range f.Vectors {
		byName[v.Name] = v.ResourceID
	}
	if byName["realistic pod container with an SDK residual"] != byName["the same, attributes in reverse order"] {
		t.Error("order changed the id")
	}
	if byName["empty"] != byName["only keys outside the covered set"] {
		t.Error("residual keys changed the id")
	}
	if byName["clickhouse reference (sql/resources.sql over the same map)"] != "18114781823887046220" {
		t.Error("the ClickHouse reference vector moved")
	}
}
