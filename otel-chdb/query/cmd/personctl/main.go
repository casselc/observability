// Command personctl is the steward's tool for the person catalog (owner
// decision O-G9, pseudonymise on departure; DECISIONS D38 items 11-17,
// research/grants.md §9.6, AMBIGUITY G6).
//
//	personctl init         -ch URL -db DB [-table T] -tenant TID
//	personctl show         -ch URL -db DB [-table T] -tenant TID -oid OID
//	personctl pseudonymise -ch URL -db DB [-table T] -tenant TID -oid OID -reason TICKET [-yes]
//
// pseudonymise reads first and stops when the oid is already pseudonymised;
// otherwise it prints the name it will hide and, with -yes, writes the
// correction (irreversible, O-G9c) with the signal's deduplication token and
// reads it back. No answer is never taken for "not applied": it re-reads and
// retries the same insert; exit 3 means the outcome is unknown and running
// the same command again is safe. The pseudonym key (at least 16 bytes) is
// read from the file named by -key-file or from $PERSONCTL_KEY (hex); in
// production it is the environment's KMS HMAC key (O-G9f, not built).
// ClickHouse credentials: $PERSONCTL_CH_USER and $PERSONCTL_CH_PASSWORD (a
// user that may insert into the table, never the query service's).
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/persons"
)

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: personctl init|show|pseudonymise [flags]")
		return 2
	}
	cmd := args[0]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	chURL := fs.String("ch", "http://127.0.0.1:8123", "ClickHouse HTTP URL")
	db := fs.String("db", "", "database of the person events table")
	table := fs.String("table", "person_events", "the person events table")
	tenant := fs.String("tenant", "", "Entra tenant id")
	oid := fs.String("oid", "", "the person's Entra object id")
	reason := fs.String("reason", "", "the offboarding ticket (required for pseudonymise)")
	keyFile := fs.String("key-file", "", "file holding the pseudonym key (else $PERSONCTL_KEY, hex)")
	yes := fs.Bool("yes", false, "write the correction (without it, pseudonymise only shows what it would do)")
	timeout := fs.Duration("timeout", 30*time.Second, "overall deadline")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cl := central.New(central.Config{URL: *chURL, User: os.Getenv("PERSONCTL_CH_USER"), Password: os.Getenv("PERSONCTL_CH_PASSWORD")})
	st, err := persons.NewStore(persons.Config{Database: *db, Table: *table, Tenant: *tenant}, cl)
	if err == nil && st == nil {
		err = errors.New("-db is required")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "personctl:", err)
		return 2
	}
	switch cmd {
	case "init":
		if err := st.CreateTable(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "personctl:", err)
			return 1
		}
		return 0
	case "show":
		evs, err := st.Events(ctx, []string{*oid})
		if err != nil {
			fmt.Fprintln(os.Stderr, "personctl:", err)
			return 1
		}
		n, _ := persons.NormaliseOID(*oid)
		now := time.Now().UnixMilli()
		ans := persons.Resolve(evs, st.Config().Tenant, []string{n}, persons.Reader{Self: n}, now, now, persons.DefaultPolicy)
		return printJSON(ans[0])
	case "pseudonymise":
		key, err := readKey(*keyFile)
		if err != nil {
			fmt.Fprintln(os.Stderr, "personctl:", err)
			return 2
		}
		if !*yes {
			evs, err := st.Events(ctx, []string{*oid})
			if err != nil {
				fmt.Fprintln(os.Stderr, "personctl:", err)
				return 1
			}
			n, _ := persons.NormaliseOID(*oid)
			now := time.Now().UnixMilli()
			a := persons.Resolve(evs, st.Config().Tenant, []string{n}, persons.Reader{Self: n}, now, now, persons.DefaultPolicy)[0]
			if a.Pseudonymised {
				fmt.Printf("%s is already pseudonymised as %s\n", n, a.Name)
				return 0
			}
			fmt.Printf("would hide %s (%q, now %s) behind %s at every basis, irreversibly; run again with -yes\n",
				n, a.Name, a.State, persons.Pseudonym(key, st.Config().Tenant, n))
			return 0
		}
		out, err := st.Pseudonymise(ctx, *oid, key, *reason, 3, 2*time.Second)
		if err != nil {
			fmt.Fprintln(os.Stderr, "personctl:", err)
			if errors.Is(err, persons.ErrUnknown) {
				return 3
			}
			return 1
		}
		if out.KeyedDiffered {
			fmt.Fprintln(os.Stderr, "personctl: the stored pseudonym was made under another key; it stays (the first correction wins)")
		}
		return printJSON(out)
	}
	fmt.Fprintln(os.Stderr, "personctl: unknown command", cmd)
	return 2
}

func readKey(file string) ([]byte, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		return []byte(strings.TrimSpace(string(b))), nil
	}
	if h := os.Getenv("PERSONCTL_KEY"); h != "" {
		return hex.DecodeString(strings.TrimSpace(h))
	}
	return nil, errors.New("no pseudonym key: -key-file or $PERSONCTL_KEY")
}

func printJSON(v any) int {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
	return 0
}
