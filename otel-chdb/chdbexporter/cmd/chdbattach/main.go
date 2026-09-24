// chdbattach is the consumer side of the chdb exporter's object storage mode,
// in miniature: it reads a published manifest (a batch's or a generation's
// _sealed.json), attaches that generation's tables read-only in its own chDB,
// and runs queries against them.
//
//	chdbattach -path ./reader -key K -secret S -manifest http://.../manifests/g.../_sealed.json \
//	    "SELECT count() FROM {table}" ["SELECT ... FROM {trace_id_ts}" ...]
//
// {table} is the attached main table, {trace_id_ts} its trace-id lookup
// table (traces only). -repeat/-every re-run the queries, to watch an active
// generation refresh. -print-ddl prints the attach statements instead, one per
// line, for running them on a ClickHouse server.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/chdb-io/chdb-go/v2/chdb"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter"
)

type manifest struct {
	Signal     string                       `json:"signal"`
	Generation string                       `json:"generation"`
	Tables     []chdbexporter.ManifestTable `json:"tables"`
}

func main() {
	path := flag.String("path", "", "chDB data directory for the reader (not the writer's)")
	key := flag.String("key", "", "S3 access key id")
	secret := flag.String("secret", "", "S3 secret access key")
	murl := flag.String("manifest", "", "URL of a batch or _sealed.json manifest")
	db := flag.String("db", "r", "database to attach into")
	refresh := flag.Int("refresh", 1, "refresh_parts_interval, seconds")
	repeat := flag.Int("repeat", 1, "times to run the queries")
	every := flag.Duration("every", time.Second, "interval between repeats")
	format := flag.String("format", "PrettyCompactMonoBlock", "output format")
	printDDL := flag.Bool("print-ddl", false, "print the attach statements, one per line, and exit")
	flag.Parse()
	if *path == "" || *murl == "" || (flag.NArg() == 0 && !*printDDL) {
		fmt.Fprintln(os.Stderr, "usage: chdbattach -path DIR -key K -secret S -manifest URL QUERY...")
		os.Exit(2)
	}
	s, err := chdb.NewSession(*path)
	check(err)
	defer s.Close()
	q := func(sql, f string) string {
		r, err := s.Query(sql, f)
		check(err)
		defer r.Free()
		return r.String()
	}
	raw := q(fmt.Sprintf("SELECT json FROM s3('%s', '%s', '%s', 'JSONAsString')", *murl, *key, *secret), "TSVRaw")
	var m manifest
	check(json.Unmarshal([]byte(raw), &m))
	cfg := chdbexporter.NewFactory().CreateDefaultConfig().(*chdbexporter.Config)
	ddl := chdbexporter.ReaderDDL(cfg, *db, m.Signal, m.Generation, m.Tables, *key, *secret, *refresh)
	if *printDDL {
		for _, stmt := range ddl {
			fmt.Println(strings.Join(strings.Fields(stmt), " "))
		}
		return
	}
	for _, stmt := range ddl {
		q(stmt, "TSV")
	}
	name := cfg.TracesTableName
	if m.Signal == "logs" {
		name = cfg.LogsTableName
	}
	table := fmt.Sprintf("%s.%s_%s", *db, name, m.Generation)
	rep := strings.NewReplacer("{table}", table, "{trace_id_ts}", table+"_trace_id_ts")
	for i := 0; i < *repeat; i++ {
		if i > 0 {
			time.Sleep(*every)
		}
		for _, sql := range flag.Args() {
			fmt.Print(q(rep.Replace(sql), *format))
		}
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
