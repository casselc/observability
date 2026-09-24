// chdbq runs statements against a chDB data directory and prints the last
// one's result:
//
//	chdbq [-format F] [-repeat N -every D] <path> <sql> [<sql>...]
//
// Every statement but the last runs once (DDL, SET); the last is printed,
// and with -repeat printed N times, D apart, which is how a test watches a
// read-only table refresh. It exists because the chdb-go CLI runs single
// queries in a throwaway session and ignores -path. Only one process can hold
// a path, so point it at a directory nothing else is using.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/chdb-io/chdb-go/v2/chdb"
)

func main() {
	format := flag.String("format", "PrettyCompactMonoBlock", "output format of the last statement")
	repeat := flag.Int("repeat", 1, "times to run the last statement")
	every := flag.Duration("every", 500*time.Millisecond, "interval between repeats")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: chdbq [-format F] [-repeat N -every D] <path> <sql> [<sql>...]")
		os.Exit(2)
	}
	s, err := chdb.NewSession(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer s.Close()
	stmts := flag.Args()[1:]
	for _, q := range stmts[:len(stmts)-1] {
		r, err := s.Query(q)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		r.Free()
	}
	last := stmts[len(stmts)-1]
	for i := 0; i < *repeat; i++ {
		if i > 0 {
			time.Sleep(*every)
		}
		r, err := s.Query(last, *format)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Stdout.Write(r.Buf())
		r.Free()
	}
}
