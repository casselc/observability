package inline

import (
	"context"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Slot is one listed object of an epoch's log.
type Slot struct {
	Seq  uint64
	Key  string
	Size int64
	ETag string
}

// Epochs lists the producer's logs (one LIST with a delimiter per 1000).
func (st *Store) Epochs(ctx context.Context) ([]string, error) {
	var out []string
	p := s3.NewListObjectsV2Paginator(st.S3, &s3.ListObjectsV2Input{
		Bucket: aws.String(st.Bucket), Prefix: aws.String(strings.TrimRight(st.Prefix, "/") + "/"), Delimiter: aws.String("/"),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			s := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(cp.Prefix), strings.TrimRight(st.Prefix, "/")+"/"), "/")
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out, nil
}

// After lists an epoch's slots from seq `from` on, in slot order: one LIST
// request per 1000 slots, StartAfter the key before `from`.
func (st *Store) After(ctx context.Context, epoch string, from uint64) ([]Slot, error) {
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(st.Bucket),
		Prefix: aws.String(strings.TrimRight(st.Prefix, "/") + "/" + epoch + "/"),
	}
	if from > 0 {
		in.StartAfter = aws.String(SlotKey(st.Prefix, epoch, from-1))
	}
	var out []Slot
	p := s3.NewListObjectsV2Paginator(st.S3, in)
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			_, seq, ok := ParseSlotKey(st.Prefix, aws.ToString(o.Key))
			if !ok {
				continue
			}
			out = append(out, Slot{Seq: seq, Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: aws.ToString(o.ETag)})
		}
	}
	return out, nil
}

// TombResult is what racing a tombstone into a slot found.
type TombResult int

const (
	TombWon  TombResult = iota // the epoch is closed at this slot
	DataWon                    // a batch got there first: ingest it and go on
	TombOpen                   // no answer and the slot reads free: try again
)

// Tombstone closes epoch at slot seq, create-only. Safe at any time: if the
// writer is alive and its batch lands first, the result is DataWon; if the
// tombstone lands first, the writer's PUT gets 412 and it halts.
func (st *Store) Tombstone(ctx context.Context, epoch string, seq uint64) (TombResult, error) {
	key := SlotKey(st.Prefix, epoch, seq)
	err := st.PutIfAbsent(ctx, key, nil, "application/octet-stream", map[string]string{MetaKind: KindTomb, MetaEpoch: epoch})
	switch Classify(err) {
	case OK:
		return TombWon, nil
	case Exists, Unknown:
		m, _, found, herr := st.Head(ctx, key)
		if herr != nil {
			return TombOpen, herr
		}
		if !found {
			return TombOpen, nil
		}
		if m[MetaKind] == KindTomb {
			return TombWon, nil
		}
		return DataWon, nil
	}
	return TombOpen, err
}
