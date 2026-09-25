// inlineconsume is a minimal central consumer for the manifest-less layout:
// it follows every epoch's log in slot order (LIST StartAfter the
// checkpoint), HEADs each slot for its content hash, checks central for it,
// inserts with ClickHouse's s3() table function, and closes superseded quiet
// epochs by racing a create-only tombstone into their first free slot.
//
// The checkpoint is kept in memory here; ../../model/S3NATIVE.md's CAS'd
// checkpoint object (If-Match) and consumer lease apply unchanged.
//
//	inlineconsume -s3 http://127.0.0.1:18333 -prefix s3inline/demo/x/traces -ch http://127.0.0.1:18123 -table default.central -once
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/awss3/inline"
)

var (
	s3url  = flag.String("s3", "http://127.0.0.1:18333", "S3 endpoint (path-style)")
	bucket = flag.String("bucket", "otel", "")
	prefix = flag.String("prefix", "", "the producer's prefix: {prefix}/{epoch}/{seq}.parquet")
	key    = flag.String("key", "otel", "")
	secret = flag.String("secret", "otelsecret", "")
	chURL  = flag.String("ch", "http://127.0.0.1:18123", "ClickHouse HTTP")
	table  = flag.String("table", "default.s3inline_central", "")
	quiet  = flag.Duration("quiet", 5*time.Second, "close a superseded epoch whose head has been free this long")
	once   = flag.Bool("once", false, "one pass, closing every superseded epoch at once")
	poll   = flag.Duration("poll", time.Second, "")
)

func ch(q string) (string, error) {
	resp, err := http.Post(*chURL+"/?"+url.Values{"query": {q}}.Encode(), "text/plain", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("clickhouse: %s", strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), nil
}

func sq(s string) string { return "'" + strings.ReplaceAll(s, "'", "\\'") + "'" }

type epochState struct {
	next     uint64
	closed   bool
	lastSeen time.Time
}

func main() {
	flag.Parse()
	ctx := context.Background()
	st := &inline.Store{S3: s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(*s3url), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(*key, *secret, "")}), Bucket: *bucket, Prefix: *prefix}
	state := map[string]*epochState{}
	var inserted, deduped, rows, tombs, tombLost int
	created := false
	for {
		epochs, err := st.Epochs(ctx)
		if err != nil {
			log.Fatal(err)
		}
		for i, ep := range epochs {
			es := state[ep]
			if es == nil {
				es = &epochState{lastSeen: time.Now()}
				state[ep] = es
			}
			for !es.closed {
				slots, err := st.After(ctx, ep, es.next)
				if err != nil {
					log.Fatal(err)
				}
				progressed := false
				for _, s := range slots {
					if s.Seq != es.next {
						break // a gap: only the head's tombstone race may resolve it
					}
					meta, _, found, err := st.Head(ctx, s.Key)
					if err != nil || !found {
						log.Fatal("head ", s.Key, err)
					}
					if meta[inline.MetaKind] == inline.KindTomb {
						es.closed = true
						break
					}
					src := fmt.Sprintf("s3(%s, %s, %s, 'Parquet')", sq(*s3url+"/"+*bucket+"/"+s.Key), sq(*key), sq(*secret))
					if !created {
						if _, err := ch(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ENGINE = MergeTree ORDER BY tuple() AS SELECT *, '' AS content_key, '' AS src FROM %s LIMIT 0", *table, src)); err != nil {
							log.Fatal(err)
						}
						created = true
					}
					h := meta[inline.MetaContent]
					n, err := ch(fmt.Sprintf("SELECT count() FROM %s WHERE content_key = %s", *table, sq(h)))
					if err != nil {
						log.Fatal(err)
					}
					if n == "0" {
						if _, err := ch(fmt.Sprintf("INSERT INTO %s SELECT *, %s, _path FROM %s SETTINGS insert_deduplication_token = %s", *table, sq(h), src, sq(h))); err != nil {
							log.Fatal(err)
						}
						inserted++
						var r int
						fmt.Sscan(meta[inline.MetaRows], &r)
						rows += r
						log.Printf("ingest %s/%d content=%s rows=%s", ep, s.Seq, h, meta[inline.MetaRows])
					} else {
						deduped++
						log.Printf("skip   %s/%d content=%s: already in central (a copy from another epoch)", ep, s.Seq, h)
					}
					es.next++
					es.lastSeen = time.Now()
					progressed = true
				}
				if progressed {
					continue
				}
				superseded := i < len(epochs)-1
				if es.closed || !superseded || (!*once && time.Since(es.lastSeen) < *quiet) {
					break
				}
				r, err := st.Tombstone(ctx, ep, es.next)
				if err != nil {
					log.Printf("tombstone %s/%d: %v", ep, es.next, err)
					break
				}
				switch r {
				case inline.TombWon:
					tombs++
					es.closed = true
					log.Printf("closed %s at slot %d (tombstone)", ep, es.next)
				case inline.DataWon:
					tombLost++
					log.Printf("tombstone %s/%d lost to a late batch: ingesting it", ep, es.next)
				}
			}
		}
		if *once {
			break
		}
		time.Sleep(*poll)
	}
	total, _ := ch("SELECT count(), uniqExact(content_key) FROM " + *table)
	log.Printf("done: inserted %d objects (%d rows), skipped %d copies, closed %d epochs, tombstones lost to data %d; central count/distinct content: %s",
		inserted, rows, deduped, tombs, tombLost, total)
}
