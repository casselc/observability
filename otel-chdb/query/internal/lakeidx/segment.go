package lakeidx

// The segment format (FORMAT.md §7.2). One create-only object per
// (cluster, signal, commit hour, batch of source objects):
//
//	"OSIX" 0x01 0x00 0x00 0x00                       magic, format 1
//	trace blocks                                     sorted (fp32, rg) entries
//	term blocks                                      sorted terms with inline postings
//	header                                           JSON (Header)
//	u32le len(header) | u32le crc32c(header) | "OSIX"
//
// A reader GETs the tail (the header, cached: segments never change), then
// ranges of blocks. Every block carries a CRC-32C in the header; a block or
// header that does not verify makes the whole segment unusable, so its
// objects are scanned (never a miss).

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"sort"
	"strings"
)

// FormatVersion is the segment format; a reader skips other versions.
const FormatVersion = 1

var (
	magic    = []byte{'O', 'S', 'I', 'X', FormatVersion, 0, 0, 0}
	trailMag = []byte("OSIX")
	castag   = crc32.MakeTable(crc32.Castagnoli)
)

// TrailerLen is the fixed trailer after the header.
const TrailerLen = 12

// SegObject is one source object a segment covers.
type SegObject struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"etag,omitempty"`
	// LastModifiedMs is the LIST's LastModified (ms): it put the object in
	// this segment's hour.
	LastModifiedMs int64 `json:"lm_ms"`
	// RowGroups is the row count of each row group, in file order; the
	// object's first row group has global ordinal Base.
	RowGroups []int64 `json:"rg_rows"`
	Base      uint32  `json:"rg_base"`
}

// TraceBlock is a fenced run of trace entries.
type TraceBlock struct {
	First uint32 `json:"first"` // the block's smallest fingerprint
	Off   int64  `json:"off"`
	Len   int64  `json:"len"`
	CRC   uint32 `json:"crc"`
	N     int    `json:"n"`
}

// TermBlock is a fenced run of dictionary entries.
type TermBlock struct {
	First string `json:"first"` // the block's smallest term
	Off   int64  `json:"off"`
	Len   int64  `json:"len"`
	CRC   uint32 `json:"crc"`
	N     int    `json:"n"`
}

// TraceSection indexes a trace-id column.
type TraceSection struct {
	Column  string       `json:"column"`
	Entries int          `json:"entries"`
	FPBits  int          `json:"fp_bits"`
	Blocks  []TraceBlock `json:"blocks"`
}

// TermSection indexes a text column's tokens.
type TermSection struct {
	Column string `json:"column"`
	Terms  int    `json:"terms"`
	// MaxTerm: longer tokens are not in the dictionary; their row groups
	// are the posting of the empty term ("", the first entry), which every
	// partial (prefix, suffix, infix) constraint unions in.
	MaxTerm int `json:"max_term"`
	// FreqCut: a term in more than this fraction of the segment's row
	// groups (with at least FreqMinRGs of them) is stored as "every row
	// group": it cannot narrow anything.
	FreqCut    float64     `json:"freq_cut"`
	FreqMinRGs int         `json:"freq_min_rgs"`
	Frequent   int         `json:"frequent"`
	Blocks     []TermBlock `json:"blocks"`
	Tokenizer  string      `json:"tokenizer"`
}

// Header describes a segment.
type Header struct {
	Format  int    `json:"format"`
	Cluster string `json:"cluster"`
	Signal  string `json:"signal"`
	Bucket  string `json:"bucket"`
	Level   int    `json:"level"`
	Builder string `json:"builder"`
	// Objects are the covered source objects; SourcesHash is sha256 over
	// "key\0size\0etag\0" of each, in order: a reader knows exactly what
	// is indexed.
	Objects     []SegObject   `json:"objects"`
	SourcesHash string        `json:"sources_hash"`
	RowGroups   int           `json:"row_groups"`
	Trace       *TraceSection `json:"trace,omitempty"`
	Terms       *TermSection  `json:"terms,omitempty"`
}

// TokenizerName names Fold's semantics (fold.go); a reader refuses a term
// section built with another tokenizer.
const TokenizerName = "jslower-ascii-word-v1"

// FP is a trace id's 32-bit fingerprint: the top 32 bits of FNV-1a 64 over
// the id's ASCII-lowercased text. A lookup returns every row group holding
// an id with the same fingerprint (a false match reads one row group more;
// about entries/2^32 per lookup).
func FP(id string) uint32 {
	h := fnv.New64a()
	var b [64]byte
	s := b[:0]
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		s = append(s, c)
	}
	_, _ = h.Write(s)
	return uint32(h.Sum64() >> 32)
}

// SourcesHash hashes a covered-object list.
func SourcesHash(objs []SegObject) string {
	h := sha256.New()
	for _, o := range objs {
		fmt.Fprintf(h, "%s\x00%d\x00%s\x00", o.Key, o.Size, o.ETag)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// BuildConfig tunes a build.
type BuildConfig struct {
	TraceBlockBytes int     // default 4096
	TermBlockBytes  int     // default 32768
	MaxTerm         int     // default 64
	FreqCut         float64 // default 0.5
	FreqMinRGs      int     // default 8
	Builder         string  // recorded in the header
}

func (c *BuildConfig) defaults() {
	if c.TraceBlockBytes <= 0 {
		c.TraceBlockBytes = 4096
	}
	if c.TermBlockBytes <= 0 {
		c.TermBlockBytes = 32 << 10
	}
	if c.MaxTerm <= 0 {
		c.MaxTerm = 64
	}
	if c.FreqCut <= 0 {
		c.FreqCut = 0.5
	}
	if c.FreqMinRGs <= 0 {
		c.FreqMinRGs = 8
	}
}

type fpEntry struct {
	fp uint32
	rg uint32
}

// Builder accumulates one segment.
type Builder struct {
	cfg      BuildConfig
	objs     []SegObject
	rgs      uint32
	traceCol string
	termCol  string
	fps      []fpEntry
	terms    map[string][]uint32
	long     []uint32
	scratch  []byte
}

// NewBuilder starts a segment; traceCol / termCol name the indexed columns
// ("" leaves that section out).
func NewBuilder(cfg BuildConfig, traceCol, termCol string) *Builder {
	cfg.defaults()
	b := &Builder{cfg: cfg, traceCol: traceCol, termCol: termCol}
	if termCol != "" {
		b.terms = map[string][]uint32{}
	}
	return b
}

// AddObject registers a covered object (its RowGroups filled) and returns
// its first row group's global ordinal.
func (b *Builder) AddObject(o SegObject) uint32 {
	o.Base = b.rgs
	b.rgs += uint32(len(o.RowGroups))
	b.objs = append(b.objs, o)
	return o.Base
}

// Objects returns the covered objects so far.
func (b *Builder) Objects() []SegObject { return b.objs }

// AddTraceID indexes one row's trace id in global row group rg.
func (b *Builder) AddTraceID(rg uint32, id string) {
	if id == "" {
		return
	}
	b.fps = append(b.fps, fpEntry{FP(id), rg})
}

// AddFP indexes a fingerprint directly (merges).
func (b *Builder) AddFP(rg uint32, fp uint32) { b.fps = append(b.fps, fpEntry{fp, rg}) }

// AddText indexes one row's text in global row group rg.
func (b *Builder) AddText(rg uint32, s string) {
	b.scratch = Tokens(b.scratch, s, func(tok []byte) {
		if len(tok) > b.cfg.MaxTerm {
			if n := len(b.long); n == 0 || b.long[n-1] != rg {
				b.long = append(b.long, rg)
			}
			return
		}
		p := b.terms[string(tok)]
		if n := len(p); n == 0 || p[n-1] != rg {
			b.terms[string(tok)] = append(p, rg)
		}
	})
}

// addPosting adds a whole posting (merges; rgs ascending, appended after
// what the term holds).
func (b *Builder) addPosting(term string, rgs []uint32) {
	if len(rgs) == 0 {
		return
	}
	if term == "" {
		b.long = appendUnique(b.long, rgs)
		return
	}
	b.terms[term] = appendUnique(b.terms[term], rgs)
}

func appendUnique(dst, rgs []uint32) []uint32 {
	for _, r := range rgs {
		if n := len(dst); n == 0 || dst[n-1] < r {
			dst = append(dst, r)
		} else if dst[n-1] != r {
			// out of order: insert (merges add in ordinal order, so rare)
			i := sort.Search(len(dst), func(i int) bool { return dst[i] >= r })
			if dst[i] != r {
				dst = append(dst, 0)
				copy(dst[i+1:], dst[i:])
				dst[i] = r
			}
		}
	}
	return dst
}

// Build encodes the segment.
func (b *Builder) Build(cluster, signal, bucket string, level int) ([]byte, *Header, error) {
	if len(b.objs) == 0 {
		return nil, nil, errors.New("lakeidx: empty segment")
	}
	h := &Header{Format: FormatVersion, Cluster: cluster, Signal: signal, Bucket: bucket, Level: level, Builder: b.cfg.Builder,
		Objects: b.objs, SourcesHash: SourcesHash(b.objs), RowGroups: int(b.rgs)}
	out := append([]byte{}, magic...)
	if b.traceCol != "" {
		sort.Slice(b.fps, func(i, j int) bool {
			if b.fps[i].fp != b.fps[j].fp {
				return b.fps[i].fp < b.fps[j].fp
			}
			return b.fps[i].rg < b.fps[j].rg
		})
		// dedupe (fp, rg)
		fps := b.fps[:0]
		for i, e := range b.fps {
			if i == 0 || e != b.fps[i-1] {
				fps = append(fps, e)
			}
		}
		b.fps = fps
		ts := &TraceSection{Column: b.traceCol, Entries: len(fps), FPBits: 32, Blocks: []TraceBlock{}}
		var blk []byte
		var cur TraceBlock
		var prev uint32
		flush := func() {
			if cur.N == 0 {
				return
			}
			cur.Off, cur.Len, cur.CRC = int64(len(out)), int64(len(blk)), crc32.Checksum(blk, castag)
			out = append(out, blk...)
			ts.Blocks = append(ts.Blocks, cur)
			blk, cur = blk[:0], TraceBlock{}
		}
		for i, e := range fps {
			// cut only where the fingerprint changes: one fingerprint is in one block
			if cur.N > 0 && len(blk) >= b.cfg.TraceBlockBytes && e.fp != fps[i-1].fp {
				flush()
			}
			if cur.N == 0 {
				cur.First, prev = e.fp, e.fp
			}
			blk = binary.AppendUvarint(blk, uint64(e.fp-prev))
			blk = binary.AppendUvarint(blk, uint64(e.rg))
			prev = e.fp
			cur.N++
		}
		flush()
		h.Trace = ts
	}
	if b.termCol != "" {
		terms := make([]string, 0, len(b.terms)+1)
		for t := range b.terms {
			terms = append(terms, t)
		}
		sort.Strings(terms)
		terms = append([]string{""}, terms...) // the long-token posting first
		tsec := &TermSection{Column: b.termCol, Terms: len(terms) - 1, MaxTerm: b.cfg.MaxTerm, FreqCut: b.cfg.FreqCut,
			FreqMinRGs: b.cfg.FreqMinRGs, Blocks: []TermBlock{}, Tokenizer: TokenizerName}
		cut := int(b.cfg.FreqCut * float64(b.rgs))
		var blk []byte
		var cur TermBlock
		var prevTerm string
		flush := func() {
			if cur.N == 0 {
				return
			}
			cur.Off, cur.Len, cur.CRC = int64(len(out)), int64(len(blk)), crc32.Checksum(blk, castag)
			out = append(out, blk...)
			tsec.Blocks = append(tsec.Blocks, cur)
			blk, cur = blk[:0], TermBlock{}
		}
		for _, t := range terms {
			if cur.N > 0 && len(blk) >= b.cfg.TermBlockBytes {
				flush()
			}
			if cur.N == 0 {
				cur.First, prevTerm = t, ""
			}
			shared := commonPrefix(prevTerm, t)
			blk = binary.AppendUvarint(blk, uint64(shared))
			blk = binary.AppendUvarint(blk, uint64(len(t)-shared))
			blk = append(blk, t[shared:]...)
			var p []uint32
			if t == "" {
				p = b.long
			} else {
				p = b.terms[t]
			}
			if t != "" && int(b.rgs) >= b.cfg.FreqMinRGs && len(p) > cut {
				blk = binary.AppendUvarint(blk, 0) // every row group
				tsec.Frequent++
			} else {
				blk = binary.AppendUvarint(blk, uint64(len(p))+1)
				var last uint32
				for i, r := range p {
					if i == 0 {
						blk = binary.AppendUvarint(blk, uint64(r))
					} else {
						blk = binary.AppendUvarint(blk, uint64(r-last))
					}
					last = r
				}
			}
			prevTerm = t
			cur.N++
		}
		flush()
		h.Terms = tsec
	}
	hj, err := json.Marshal(h)
	if err != nil {
		return nil, nil, err
	}
	out = append(out, hj...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(hj)))
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(hj, castag))
	out = append(out, trailMag...)
	return out, h, nil
}

func commonPrefix(a, b string) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// ErrCorrupt marks a segment that does not verify.
var ErrCorrupt = errors.New("lakeidx: corrupt segment")

func corrupt(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, a...))
}

// ParseTail reads the header from a segment's last bytes (tail) given the
// object's size. need > 0 means the tail was too short: fetch the last
// need bytes and call again.
func ParseTail(tail []byte, size int64) (h *Header, need int64, err error) {
	if size < int64(len(magic)+TrailerLen) || int64(len(tail)) > size {
		return nil, 0, corrupt("size %d", size)
	}
	if len(tail) < TrailerLen {
		return nil, TrailerLen, nil
	}
	tr := tail[len(tail)-TrailerLen:]
	if !bytes.Equal(tr[8:], trailMag) {
		return nil, 0, corrupt("no trailer")
	}
	hl := int64(binary.LittleEndian.Uint32(tr[0:4]))
	if hl+int64(TrailerLen+len(magic)) > size {
		return nil, 0, corrupt("header length %d of %d", hl, size)
	}
	if int64(len(tail)) < hl+TrailerLen {
		return nil, hl + TrailerLen, nil
	}
	hj := tail[int64(len(tail))-TrailerLen-hl : len(tail)-TrailerLen]
	if crc32.Checksum(hj, castag) != binary.LittleEndian.Uint32(tr[4:8]) {
		return nil, 0, corrupt("header checksum")
	}
	var hd Header
	if err := json.Unmarshal(hj, &hd); err != nil {
		return nil, 0, corrupt("header: %v", err)
	}
	if hd.Format != FormatVersion {
		return nil, 0, fmt.Errorf("lakeidx: segment format %d, this reader reads %d", hd.Format, FormatVersion)
	}
	bodyEnd := size - hl - TrailerLen
	if err := hd.check(bodyEnd); err != nil {
		return nil, 0, err
	}
	return &hd, 0, nil
}

// check validates the header's internal consistency: ordinals tile, blocks
// lie inside the body in order.
func (h *Header) check(bodyEnd int64) error {
	var base uint32
	for i, o := range h.Objects {
		if o.Base != base {
			return corrupt("object %d base %d, want %d", i, o.Base, base)
		}
		base += uint32(len(o.RowGroups))
	}
	if int(base) != h.RowGroups {
		return corrupt("row groups %d, objects hold %d", h.RowGroups, base)
	}
	if SourcesHash(h.Objects) != h.SourcesHash {
		return corrupt("sources hash")
	}
	end := int64(len(magic))
	inBody := func(off, n int64) bool {
		ok := off >= end && n > 0 && off+n <= bodyEnd
		end = off + n
		return ok
	}
	if h.Trace != nil {
		for i, b := range h.Trace.Blocks {
			if !inBody(b.Off, b.Len) || (i > 0 && b.First <= h.Trace.Blocks[i-1].First) {
				return corrupt("trace block %d", i)
			}
		}
	}
	if h.Terms != nil {
		if h.Terms.Tokenizer != TokenizerName {
			return fmt.Errorf("lakeidx: tokenizer %q, this reader folds as %q", h.Terms.Tokenizer, TokenizerName)
		}
		for i, b := range h.Terms.Blocks {
			if !inBody(b.Off, b.Len) || (i > 0 && b.First <= h.Terms.Blocks[i-1].First) {
				return corrupt("term block %d", i)
			}
		}
		if len(h.Terms.Blocks) == 0 || h.Terms.Blocks[0].First != "" {
			return corrupt("term section without the long-token entry")
		}
	}
	return nil
}

// Object returns the covered object holding global row group rg and the
// row group's index inside it.
func (h *Header) Object(rg uint32) (int, int) {
	i := sort.Search(len(h.Objects), func(i int) bool { return h.Objects[i].Base > rg }) - 1
	if i < 0 {
		return -1, -1
	}
	return i, int(rg - h.Objects[i].Base)
}

// TraceBlockFor returns the index of the block that holds fp's entries, or -1.
func (h *Header) TraceBlockFor(fp uint32) int {
	if h.Trace == nil {
		return -1
	}
	bs := h.Trace.Blocks
	return sort.Search(len(bs), func(i int) bool { return bs[i].First > fp }) - 1
}

// LookupTrace returns the row groups the block lists for fp.
func LookupTrace(blk []byte, b TraceBlock, fp uint32) ([]uint32, error) {
	if crc32.Checksum(blk, castag) != b.CRC {
		return nil, corrupt("trace block checksum")
	}
	var out []uint32
	cur := b.First
	for n, i := 0, 0; n < b.N; n++ {
		d, k := binary.Uvarint(blk[i:])
		if k <= 0 {
			return nil, corrupt("trace block entry %d", n)
		}
		i += k
		rg, k2 := binary.Uvarint(blk[i:])
		if k2 <= 0 {
			return nil, corrupt("trace block entry %d", n)
		}
		i += k2
		if n == 0 {
			if d != 0 {
				return nil, corrupt("trace block first delta")
			}
		}
		cur += uint32(d)
		if cur == fp {
			out = append(out, uint32(rg))
		} else if cur > fp {
			break
		}
	}
	return out, nil
}

// TermBlocksFor returns the block range [lo, hi) a constraint must read.
// Full: the one block that may hold the term; Prefix: the blocks spanning
// terms that start with it, plus block 0 (the long-token entry); Suffix
// and Infix: every block.
func (h *Header) TermBlocksFor(c Constraint) (lo, hi int, withFirst bool) {
	bs := h.Terms.Blocks
	at := func(s string) int { return sort.Search(len(bs), func(i int) bool { return bs[i].First > s }) - 1 }
	switch c.Match {
	case Full:
		if len(c.Text) > h.Terms.MaxTerm {
			return 0, 1, false // a token that long is in the long-token entry only
		}
		i := at(c.Text)
		return i, i + 1, false
	case Prefix:
		lo := at(c.Text)
		hi := at(c.Text+"\xff") + 1
		return lo, hi, lo > 0
	default:
		return 0, len(bs), false
	}
}

// Selects says whether dictionary entry term answers constraint c: the
// long-token entry ("") answers every constraint a token longer than
// maxTerm could satisfy.
func Selects(c Constraint, term string, maxTerm int) bool {
	if term == "" {
		return c.Match != Full || len(c.Text) > maxTerm
	}
	return c.Holds(term)
}

// ScanTermBlock walks one verified block and calls f on every entry.
func ScanTermBlock(blk []byte, b TermBlock, f func(term string, all bool, rgs []uint32) bool) error {
	if crc32.Checksum(blk, castag) != b.CRC {
		return corrupt("term block checksum")
	}
	var prev []byte
	i := 0
	for n := 0; n < b.N; n++ {
		shared, k := binary.Uvarint(blk[i:])
		if k <= 0 || int(shared) > len(prev) {
			return corrupt("term entry %d", n)
		}
		i += k
		sl, k := binary.Uvarint(blk[i:])
		if k <= 0 || uint64(len(blk)-i-k) < sl {
			return corrupt("term entry %d", n)
		}
		i += k
		term := append(prev[:shared:shared], blk[i:i+int(sl)]...)
		i += int(sl)
		cnt, k := binary.Uvarint(blk[i:])
		if k <= 0 {
			return corrupt("term entry %d", n)
		}
		i += k
		var rgs []uint32
		all := cnt == 0
		if !all {
			var last uint32
			for j := uint64(0); j < cnt-1; j++ {
				v, k := binary.Uvarint(blk[i:])
				if k <= 0 {
					return corrupt("term entry %d posting", n)
				}
				i += k
				if j == 0 {
					last = uint32(v)
				} else {
					last += uint32(v)
				}
				rgs = append(rgs, last)
			}
		}
		prev = term
		if !f(string(term), all, rgs) {
			return nil
		}
	}
	if i != len(blk) {
		return corrupt("term block trailing bytes")
	}
	return nil
}

// Segment is a fully decoded segment (merges and tests).
type Segment struct {
	Header *Header
	FPs    []fpEntry
	Terms  map[string][]uint32 // nil posting: every row group
	Long   []uint32
}

// Decode verifies and decodes a whole segment.
func Decode(body []byte) (*Segment, error) {
	h, need, err := ParseTail(body, int64(len(body)))
	if err != nil {
		return nil, err
	}
	if need > 0 || !bytes.Equal(body[:len(magic)], magic) {
		return nil, corrupt("magic")
	}
	s := &Segment{Header: h}
	if h.Trace != nil {
		for _, b := range h.Trace.Blocks {
			blk := body[b.Off : b.Off+b.Len]
			if crc32.Checksum(blk, castag) != b.CRC {
				return nil, corrupt("trace block checksum")
			}
			cur := b.First
			for n, i := 0, 0; n < b.N; n++ {
				d, k := binary.Uvarint(blk[i:])
				if k <= 0 {
					return nil, corrupt("trace entry")
				}
				i += k
				rg, k2 := binary.Uvarint(blk[i:])
				if k2 <= 0 {
					return nil, corrupt("trace entry")
				}
				i += k2
				cur += uint32(d)
				s.FPs = append(s.FPs, fpEntry{cur, uint32(rg)})
			}
		}
	}
	if h.Terms != nil {
		s.Terms = map[string][]uint32{}
		for _, b := range h.Terms.Blocks {
			err := ScanTermBlock(body[b.Off:b.Off+b.Len], b, func(t string, all bool, rgs []uint32) bool {
				switch {
				case t == "":
					s.Long = rgs
				case all:
					s.Terms[t] = nil
				default:
					s.Terms[t] = rgs
				}
				return true
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}

// Key is a segment's key: {root}/{cluster}/_index/v1/{signal}/{bucket}/L{level}-{sha256(body)[:32]}.osix.
// Content-addressed, so two indexers that build the same bytes race to the
// same create-only key, and a 412 means the bytes are already there.
func Key(root, cluster, signal, bucket string, level int, body []byte) string {
	sum := sha256.Sum256(body)
	return join(root, fmt.Sprintf("%s/%s/%s/%s/L%d-%s.osix", cluster, IndexDir, signal, bucket, level, hex.EncodeToString(sum[:16])))
}

// IndexDir is the per-cluster directory of index objects. It starts with
// '_' so no lane walk (consumer, GC, watermark, planner) takes it for a
// producer (FORMAT.md §1: names never start with '_').
const IndexDir = "_index/v1"

// BucketPrefix is the prefix of one hour's segments.
func BucketPrefix(root, cluster, signal, bucket string) string {
	return join(root, fmt.Sprintf("%s/%s/%s/%s/", cluster, IndexDir, signal, bucket))
}

// ProgressKey is the indexer's progress document for (cluster, signal).
func ProgressKey(root, cluster, signal string) string {
	return join(root, fmt.Sprintf("%s/%s/%s.progress.json", cluster, IndexDir, signal))
}

func join(a, b string) string {
	a = strings.Trim(a, "/")
	if a == "" {
		return b
	}
	return a + "/" + b
}
