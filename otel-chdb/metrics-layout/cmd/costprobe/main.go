// costprobe runs statements over the native protocol and prints each one's
// server cost (ProfileEvents): CPU, rows/bytes read, rows written, wall.
// Statements come from -f FILE (separated by ";" at line end, "--" comments
// skipped) or from the arguments; {db} is replaced with -db.
//
//	costprobe -db ml_main -f sql/rollup.sql
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
)

func main() {
	db := flag.String("db", "ml_main", "database for {db}")
	file := flag.String("f", "", "statements file")
	flag.Parse()
	var stmts []string
	if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			log.Fatal(err)
		}
		var cur []string
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), "--") {
				continue
			}
			cur = append(cur, l)
			if strings.HasSuffix(strings.TrimSpace(l), ";") {
				s := strings.TrimSuffix(strings.TrimSpace(strings.Join(cur, "\n")), ";")
				if s != "" {
					stmts = append(stmts, s)
				}
				cur = nil
			}
		}
	}
	stmts = append(stmts, flag.Args()...)
	ctx := context.Background()
	c, err := central.Dial(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range stmts {
		s = strings.ReplaceAll(s, "{db}", *db)
		cost, err := c.Exec(ctx, s, "", nil)
		first := strings.Join(strings.Fields(s), " ")
		if len(first) > 110 {
			first = first[:110]
		}
		if err != nil {
			log.Fatalf("%s: %v", first, err)
		}
		fmt.Printf("cpu_ms=%.0f wall_ms=%d read_rows=%d read_bytes=%d written_rows=%d | %s\n", cost.CPUus/1e3, cost.Wall.Milliseconds(),
			cost.ReadRows, cost.ReadBytes, cost.Events["InsertedRows"], first)
	}
}
