package chdbexporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/chdb-io/chdb-go/v2/chdb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// session is the slice of *chdb.Session the exporter uses. Insert exists
// only on the fork; stock chdb-go sessions are adapted by stockSession in
// session_stock.go when built with -tags stockchdb.
type session interface {
	Query(q string, formats ...string) (result, error)
	Insert(query, format string, data []byte) error
	Close()
}

type result interface{ Free() }

type chdbExporter struct {
	cfg    *Config
	logger *zap.Logger

	// conns is a pool: a native connection runs one statement at a time, and
	// the sending queue may call push from several goroutines.
	conns chan session
	all   []session

	tracesSQL, logsSQL         string // INSERT ... (cols), no FORMAT
	tracesFileSQL, logsFileSQL string // INSERT ... SELECT FROM file(%s)
	tracesJSONSQL, logsJSONSQL string // INSERT ... FORMAT JSONEachRow\n
	rbPool, jsonPool           sync.Pool
	fileSeq                    atomic.Uint64
}

func newExporter(logger *zap.Logger, cfg *Config) *chdbExporter {
	e := &chdbExporter{cfg: cfg, logger: logger}
	tt := insertTarget(cfg, cfg.TracesTableName)
	lt := insertTarget(cfg, cfg.LogsTableName)
	tc, lc := quoteColumns(traceColumns), quoteColumns(logColumns)
	e.tracesSQL = fmt.Sprintf("INSERT INTO %s (%s)", tt, tc)
	e.logsSQL = fmt.Sprintf("INSERT INTO %s (%s)", lt, lc)
	e.tracesJSONSQL = e.tracesSQL + " FORMAT JSONEachRow\n"
	e.logsJSONSQL = e.logsSQL + " FORMAT JSONEachRow\n"
	e.tracesFileSQL = e.tracesSQL + " SELECT * FROM file(%s, 'RowBinary', " + sqlQuote(traceStructure) + ")"
	e.logsFileSQL = e.logsSQL + " SELECT * FROM file(%s, 'RowBinary', " + sqlQuote(logStructure) + ")"
	e.rbPool.New = func() any { return &rowBinary{buf: make([]byte, 0, 1<<20)} }
	e.jsonPool.New = func() any { return &jsonEachRow{buf: make([]byte, 0, 1<<20)} }
	return e
}

func sqlQuote(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func (e *chdbExporter) start(_ context.Context, _ component.Host) error {
	if e.cfg.InsertFormat == FormatRowBinary && !insertSupported {
		return errors.New("insert_format rowbinary needs the chdb-go fork with Session.Insert; this build uses stock chdb-go, so use json or file")
	}
	e.conns = make(chan session, e.cfg.Connections)
	for i := 0; i < e.cfg.Connections; i++ {
		s, err := openSession(e.cfg.Path)
		if err != nil {
			e.closeAll()
			return fmt.Errorf("open chDB at %q: %w", e.cfg.Path, err)
		}
		e.all = append(e.all, s)
		e.conns <- s
	}
	if e.cfg.CreateSchema {
		s := e.all[0]
		for _, q := range schemaDDL(e.cfg) {
			if err := exec(s, q); err != nil {
				e.closeAll()
				return fmt.Errorf("create schema: %w", err)
			}
		}
	}
	return nil
}

func (e *chdbExporter) shutdown(context.Context) error {
	if len(e.all) == 0 {
		return nil
	}
	var err error
	if e.cfg.BufferSeconds > 0 {
		for _, t := range []string{e.cfg.TracesTableName, e.cfg.LogsTableName} {
			err = errors.Join(err, exec(e.all[0], fmt.Sprintf("OPTIMIZE TABLE %s.%s_buf", e.cfg.Database, t)))
		}
	}
	e.closeAll()
	return err
}

func (e *chdbExporter) closeAll() {
	for _, s := range e.all {
		s.Close()
	}
	e.all = nil
}

func exec(s session, q string) error {
	r, err := s.Query(q)
	if err != nil {
		return err
	}
	if r != nil {
		r.Free()
	}
	return nil
}

func (e *chdbExporter) pushTraces(_ context.Context, td ptrace.Traces) error {
	return e.push("traces", td.SpanCount(),
		func(w rowWriter) int { return writeTraces(w, td) },
		traceColumns, e.tracesSQL, e.tracesJSONSQL, e.tracesFileSQL)
}

func (e *chdbExporter) pushLogs(_ context.Context, ld plog.Logs) error {
	return e.push("logs", ld.LogRecordCount(),
		func(w rowWriter) int { return writeLogs(w, ld) },
		logColumns, e.logsSQL, e.logsJSONSQL, e.logsFileSQL)
}

func (e *chdbExporter) push(signal string, count int, write func(rowWriter) int, cols []string, insertSQL, jsonSQL, fileSQL string) error {
	if count == 0 {
		return nil
	}
	start := time.Now()
	var (
		rows int
		data []byte
		err  error
	)
	s := <-e.conns
	defer func() { e.conns <- s }()
	switch e.cfg.InsertFormat {
	case FormatRowBinary:
		w := e.rbPool.Get().(*rowBinary)
		w.buf = w.buf[:0]
		rows = write(w)
		data = w.buf
		encoded := time.Since(start)
		err = s.Insert(insertSQL, "RowBinary", w.buf)
		e.debug(signal, rows, len(data), encoded, start)
		e.rbPool.Put(w)
	case FormatJSON:
		w := e.jsonPool.Get().(*jsonEachRow)
		w.buf = append(w.buf[:0], jsonSQL...)
		w.cols = cols
		rows = write(w)
		data = w.buf
		encoded := time.Since(start)
		// The statement and its rows are one string; unsafeString spares a
		// copy, and purego copies it once more into a C string anyway.
		err = exec(s, unsafeString(w.buf))
		e.debug(signal, rows, len(data), encoded, start)
		e.jsonPool.Put(w)
	case FormatFile:
		w := e.rbPool.Get().(*rowBinary)
		w.buf = w.buf[:0]
		rows = write(w)
		data = w.buf
		encoded := time.Since(start)
		err = e.insertFile(s, fileSQL, w.buf)
		e.debug(signal, rows, len(data), encoded, start)
		e.rbPool.Put(w)
	}
	if err != nil {
		return fmt.Errorf("chdb insert %d %s: %w", rows, signal, err)
	}
	return nil
}

func (e *chdbExporter) insertFile(s session, sqlFmt string, data []byte) error {
	dir := e.cfg.FileDir
	if dir == "" {
		dir = os.TempDir()
	}
	p := filepath.Join(dir, fmt.Sprintf("chdbexporter-%d-%d.rowbinary", os.Getpid(), e.fileSeq.Add(1)))
	if err := os.WriteFile(p, data, 0o600); err != nil {
		return err
	}
	defer os.Remove(p)
	return exec(s, fmt.Sprintf(sqlFmt, sqlQuote(p)))
}

func (e *chdbExporter) debug(signal string, rows, bytes int, encoded time.Duration, start time.Time) {
	if ce := e.logger.Check(zap.DebugLevel, "chdb insert"); ce != nil {
		ce.Write(zap.String("signal", signal), zap.Int("rows", rows), zap.Int("bytes", bytes),
			zap.Duration("encode", encoded), zap.Duration("total", time.Since(start)))
	}
}

func unsafeString(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }

// chdbSession adapts *chdb.Session to session.
type chdbSession struct{ *chdb.Session }

func (s chdbSession) Query(q string, formats ...string) (result, error) {
	r, err := s.Session.Query(q, formats...)
	if err != nil || r == nil {
		return nil, err
	}
	return r, nil
}
