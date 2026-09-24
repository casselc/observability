// chdbq runs one query against a chDB data directory and prints the result:
//
//	chdbq ./data/chdb "SELECT count() FROM otel.otel_traces" [format]
//
// It exists because the chdb-go CLI runs single queries in a throwaway
// session and ignores -path. Only one process can hold a path, so run it
// while the collector is stopped.
package main

import (
	"fmt"
	"os"

	"github.com/chdb-io/chdb-go/v2/chdb"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: chdbq <path> <sql> [format]")
		os.Exit(2)
	}
	format := "PrettyCompactMonoBlock"
	if len(os.Args) > 3 {
		format = os.Args[3]
	}
	s, err := chdb.NewSession(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer s.Close()
	r, err := s.Query(os.Args[2], format)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Stdout.Write(r.Buf())
}
