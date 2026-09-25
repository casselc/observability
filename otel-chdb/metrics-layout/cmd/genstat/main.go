// genstat prints the fleet's shape and how fast it generates.
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/fleet"
)

func main() {
	rounds := flag.Int("rounds", 3, "rounds to generate")
	flag.Parse()
	cfg := fleet.Default()
	f := fleet.New(cfg)
	fmt.Printf("pods %d, series/pod %d, points/pod by type %v, series %d\n", f.Pods(), fleet.SeriesPerPod(), fleet.PointsPerPod(), f.Pods()*fleet.SeriesPerPod())
	t0 := time.Now()
	pts := 0
	for r := 0; r < *rounds; r++ {
		md := f.EmitRound()
		pts += md.DataPointCount()
	}
	d := time.Since(t0)
	fmt.Printf("%d points in %v: %.2f us/point\n", pts, d, float64(d.Microseconds())/float64(pts))
}
