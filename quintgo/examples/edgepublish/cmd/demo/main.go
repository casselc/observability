// Command demo drives the stand-in publisher through one scenario and emits
// the model steps it records, in any of three forms:
//
//	-out   OTLP/JSON spans, as the collector's file exporter writes (via OTel)
//	-steps the native step log: one JSON line per step (no OTel)
//	-itf   a model-level ITF trace per actor, in `quint run --mbt` shape,
//	       with the binding applied in-process (needs -binding)
//
// Build it with `orchestrion go build` so the //quint:action annotations
// record steps; a plain build records nothing.
//
//	demo -scenario happy -out spans.jsonl -steps steps.jsonl -itf itf/ -binding binding.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/chucklehead-dev/oscope/spike/quintgo/binding"
	"github.com/chucklehead-dev/oscope/spike/quintgo/examples/edgepublish/scenarios"
	"github.com/chucklehead-dev/oscope/spike/quintgo/otelio"
	"github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	scenario := flag.String("scenario", "happy", "scenario to run")
	out := flag.String("out", "", "OTLP/JSON output file (spans through the OTel SDK)")
	steps := flag.String("steps", "", "native step log output file (no OTel)")
	itfDir := flag.String("itf", "", "directory for model-level ITF traces, one per actor (needs -binding)")
	bpath := flag.String("binding", "", "binding file, for -itf")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: demo -scenario NAME [-out spans.jsonl] [-steps steps.jsonl] [-itf DIR -binding binding.yaml]\nscenarios:\n")
		for _, n := range scenarios.Names {
			fmt.Fprintf(os.Stderr, "  %-15s %s\n", n, scenarios.Describe[n])
		}
	}
	flag.Parse()
	if _, ok := scenarios.Describe[*scenario]; !ok || (*out == "" && *steps == "" && *itfDir == "") || (*itfDir != "" && *bpath == "") {
		flag.Usage()
		os.Exit(2)
	}

	var sinks qobs.Tee
	var tp *sdktrace.TracerProvider
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		// Every span kept: a sampled-out step is a hole in the model trace.
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithBatcher(otelio.NewFileExporter(f)),
			sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "edgepublish-demo"))),
		)
		sinks = append(sinks, qobs.NewOTelSink(tp))
	}
	var stepLog *qobs.JSONLSink
	if *steps != "" {
		var err error
		if stepLog, err = qobs.FileSink(*steps); err != nil {
			log.Fatal(err)
		}
		sinks = append(sinks, stepLog)
	}
	mem := &qobs.MemorySink{}
	if *itfDir != "" {
		sinks = append(sinks, mem)
	}
	qobs.SetGlobal(qobs.NewRecorder(sinks))

	scenarios.Run(*scenario, os.Stdout)
	qobs.SetGlobal(nil)

	fmt.Printf("scenario %s: %s\n", *scenario, scenarios.Describe[*scenario])
	if tp != nil {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s (OTLP/JSON)\n", *out)
	}
	if stepLog != nil {
		if err := stepLog.Close(); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("wrote %s (step log)\n", *steps)
	}
	if *itfDir != "" {
		b, err := binding.Load(*bpath)
		if err != nil {
			log.Fatal(err)
		}
		traces, issues, err := b.StepsToITF(mem.Steps())
		for _, i := range issues {
			fmt.Println("issue:", i)
		}
		if err != nil {
			log.Fatal(err)
		}
		if err := os.MkdirAll(*itfDir, 0o755); err != nil {
			log.Fatal(err)
		}
		for actor, data := range traces {
			p := filepath.Join(*itfDir, strings.ReplaceAll(actor, "/", "_")+".itf.json")
			if err := os.WriteFile(p, data, 0o644); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("wrote %s (model-level ITF, actor %s)\n", p, actor)
		}
	}
}
