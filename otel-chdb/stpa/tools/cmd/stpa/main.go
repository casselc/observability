// Command stpa checks the STPA records and renders their views.
//
//	stpa check   [-root DIR]            validate records and views; fail if a generated file is stale
//	stpa render  [-root DIR]            (re)write every generated file
//	stpa compare [-root DIR] [-docs DIR] compare generated tables with the hand-kept documents
//	stpa export-v0 [-root DIR] OUT      write a stpa-workbench artifact-v0 project (a derived view)
//
// DIR defaults to the stpa directory above the tool (otel-chdb/stpa).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/casselc/observability/otel-chdb/stpa/tools/internal/stpa"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	root := fs.String("root", defaultRoot(), "the stpa directory (project.yaml, records/, views/)")
	docs := fs.String("docs", "", "where the hand-kept documents live (default: ROOT/..)")
	_ = fs.Parse(os.Args[2:])
	p, err := stpa.Load(*root)
	if err != nil {
		fail(err)
	}
	switch cmd {
	case "check":
		errs := 0
		for _, f := range p.Check() {
			fmt.Println(f)
			if !f.Warn {
				errs++
			}
		}
		for _, s := range p.Stale() {
			fmt.Println("stale:", s)
			errs++
		}
		fmt.Printf("%d records, %d views, %d errors\n", len(p.Records), len(p.Views), errs)
		if errs > 0 {
			os.Exit(1)
		}
	case "render":
		for _, f := range p.Check() {
			if !f.Warn {
				fail(fmt.Errorf("records do not check; run stpa check (%s)", f))
			}
		}
		written, err := p.Write()
		if err != nil {
			fail(err)
		}
		for _, w := range written {
			fmt.Println("wrote", w)
		}
	case "compare":
		d := *docs
		if d == "" {
			d = filepath.Join(*root, "..")
		}
		ds, summary := p.Compare(d)
		for _, x := range ds {
			fmt.Println(x)
		}
		for _, s := range summary {
			fmt.Println(s)
		}
	case "export-v0":
		if fs.NArg() != 1 {
			usage()
		}
		if err := p.ExportV0(fs.Arg(0)); err != nil {
			fail(err)
		}
	default:
		usage()
	}
}

func defaultRoot() string {
	for _, c := range []string{".", "..", "../..", "otel-chdb/stpa", "stpa"} {
		if _, err := os.Stat(filepath.Join(c, "project.yaml")); err == nil {
			if _, err := os.Stat(filepath.Join(c, "records")); err == nil {
				return c
			}
		}
	}
	return "."
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: stpa check|render|compare|export-v0 [-root DIR] [-docs DIR] [OUT]")
	os.Exit(2)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "stpa:", err)
	os.Exit(1)
}
