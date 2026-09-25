package central

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// NativeAddr is the server's native port (ProfileEvents come only over it).
var NativeAddr = env("ML_CLICKHOUSE_NATIVE", "127.0.0.1:19000")

// Cost is one statement's server-side cost, from the ProfileEvents stream
// (the query's thread-group rows) and the progress packets.
type Cost struct {
	CPUus     float64 // OSCPUVirtualTimeMicroseconds
	UserSysus float64 // UserTimeMicroseconds + SystemTimeMicroseconds
	ReadRows  uint64
	ReadBytes uint64
	Wall      time.Duration
	Events    map[string]int64 // every event, thread-group totals
	PerThread map[string]int64 // same, summed over the per-thread rows
}

// Conn is one native connection.
type Conn struct{ c *ch.Client }

// Dial opens a native connection.
func Dial(ctx context.Context) (*Conn, error) {
	c, err := ch.Dial(ctx, ch.Options{Address: NativeAddr, DialTimeout: 10 * time.Second, Compression: ch.CompressionDisabled})
	if err != nil {
		return nil, err
	}
	return &Conn{c}, nil
}

// Close closes it.
func (c *Conn) Close() error { return c.c.Close() }

// Exec runs sql with settings and returns its cost; result rows are discarded.
func (c *Conn) Exec(ctx context.Context, sql, queryID string, settings map[string]string) (Cost, error) {
	cost := Cost{Events: map[string]int64{}, PerThread: map[string]int64{}}
	var ss []ch.Setting
	for k, v := range settings {
		ss = append(ss, ch.Setting{Key: k, Value: v, Important: true})
	}
	t0 := time.Now()
	err := c.c.Do(ctx, ch.Query{
		Body:     sql,
		QueryID:  queryID,
		Settings: ss,
		Result:   (&proto.Results{}).Auto(),
		OnProgress: func(ctx context.Context, p proto.Progress) error {
			cost.ReadRows += p.Rows
			cost.ReadBytes += p.Bytes
			return nil
		},
		OnProfileEvents: func(ctx context.Context, es []ch.ProfileEvent) error {
			for _, e := range es {
				if e.Type != proto.ProfileIncrement {
					continue
				}
				if e.ThreadID == 0 {
					cost.Events[e.Name] += e.Value
				} else {
					cost.PerThread[e.Name] += e.Value
				}
			}
			return nil
		},
	})
	cost.Wall = time.Since(t0)
	cost.CPUus = float64(cost.Events["OSCPUVirtualTimeMicroseconds"])
	cost.UserSysus = float64(cost.Events["UserTimeMicroseconds"] + cost.Events["SystemTimeMicroseconds"])
	return cost, err
}
