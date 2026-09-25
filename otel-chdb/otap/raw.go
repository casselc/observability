package otap

import (
	"context"
	"fmt"
	"strings"
	"time"

	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
)

// Raw stores each OTAP payload exactly as received: the Arrow IPC stream
// bytes of the BatchArrowRecords (zstd-compressed buffers, dictionaries,
// delta-encoded ids), one `.arrows` object per payload type. ClickHouse
// reads them with the ArrowStream format; the central SQL decodes the ids.
// The edge does no Arrow work at all.
//
// It needs a self-contained IPC stream per object: the BatchArrowRecords
// must start its streams (schema and dictionaries included), which holds for
// the first batch of a producer or after a schema reset. A long-lived gRPC
// stream sends schemas once, so a relaying edge would have to re-encode or
// prepend the stream's schema and dictionary messages.
const Raw = "raw"

// PublishRaw writes the payloads and a manifest.
func (p *Publisher) PublishRaw(ctx context.Context, bar *pb.BatchArrowRecords) (Manifest, error) {
	signal := "logs"
	for _, pl := range bar.ArrowPayloads {
		if pl.Type == Spans {
			signal = "traces"
		}
	}
	env, t := p.envelope(signal)
	m := p.manifest(signal, Raw, env, t, -1)
	for _, pl := range bar.ArrowPayloads {
		if err := p.put(ctx, &m, signal, Raw, env.Batch, TableName(pl.Type), -1, pl.Record, ".arrows"); err != nil {
			return m, err
		}
	}
	m.ContentKey = ContentKey(bar)
	return m, p.commit(ctx, m)
}

// RawSource is one raw table as the central query sees it: its FROM source
// and the top-level columns it has (from DESCRIBE, since OTAP columns are
// optional and ArrowStream has no explicit-structure fallback for Tuples).
type RawSource struct {
	From string
	Cols map[string]bool
}

// RawEnvelope supplies what the raw objects do not carry: producer, epoch,
// schema version, and each batch's received_at, from the manifests.
type RawEnvelope struct {
	Producer, Epoch string
	Schema          uint16
	Received        map[uint64]time.Time
}

func (e RawEnvelope) sql(batch string) string {
	var ids, ts []string
	for id, t := range e.Received {
		ids = append(ids, fmt.Sprint(id))
		ts = append(ts, fmt.Sprintf("toDateTime64('%s', 9, 'UTC')", t.UTC().Format("2006-01-02 15:04:05.000000000")))
	}
	return fmt.Sprintf("'%s' AS producer_id, '%s' AS producer_epoch, %s AS batch_id, toUInt32(rn0) AS row_ordinal, "+
		"transform(%s, [%s], [%s], toDateTime64(0, 9, 'UTC')) AS received_at, toUInt16(%d) AS schema_version",
		e.Producer, e.Epoch, batch, batch, strings.Join(ids, ", "), strings.Join(ts, ", "), e.Schema)
}

// numbered adds batch_id (from the object path) and rn (row order within
// the object). Row order is only meaningful single-threaded: the query must
// run with max_threads = 1.
func numbered(src RawSource) string {
	return fmt.Sprintf("(SELECT *, toUInt64(extract(_path, '/([0-9]{20})/')) AS batch_id, rowNumberInAllBlocks() AS rn FROM %s)", src.From)
}

func (s RawSource) col(name string) string {
	if s.Cols[name] {
		return name
	}
	return "NULL"
}

// qcol is col qualified by a table alias.
func (s RawSource) qcol(alias, name string) string {
	if s.Cols[name] {
		return alias + "." + name
	}
	return "NULL"
}

const wAll = "(PARTITION BY batch_id ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

// rawAttrs decodes an attribute table's quasi-delta parent_id (a delta while
// type, key and value repeat, for str/int/double/bool/bytes values; absolute
// otherwise) and aggregates one Map per (batch_id, parent). CBOR values
// (maps, slices) cannot be rendered in SQL: they come out as ”.
func rawAttrs(s RawSource) string {
	if s.From == "" {
		return "(SELECT toUInt64(0) AS batch_id, toUInt64(0) AS parent_id, map('', '') AS m WHERE 0)"
	}
	val := fmt.Sprintf("toString(tuple(%s, %s, %s, %s, %s))", s.col("str"), s.col("int"), s.col("double"), s.col("bool"), s.col("bytes"))
	render := fmt.Sprintf("ifNull(multiIf(type = 1, ifNull(toString(%[1]s), ''), type = 2, toString(%[2]s), type = 3, %[3]s, type = 4, if(%[4]s, 'true', 'false'), type = 7, base64Encode(ifNull(%[5]s, '')), ''), '')",
		s.col("str"), s.col("int"), dblSQL(s.col("double")), s.col("bool"), s.col("bytes"))
	return fmt.Sprintf(`(SELECT batch_id, parent, mapFromArrays(groupArray(key), groupArray(v)) AS m FROM (
	  SELECT batch_id, rn, key, v, sum(pid) OVER (PARTITION BY batch_id, run ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS parent FROM (
	    SELECT batch_id, rn, key, v, pid, sum(brk) OVER %[4]s AS run FROM (
	      SELECT batch_id, rn, toString(key) AS key, %[2]s AS v, toUInt64(parent_id) AS pid,
	        NOT (type IN (1, 2, 3, 4, 7) AND row_number() OVER %[4]s > 1 AND type = lagInFrame(type) OVER %[4]s
	             AND key = lagInFrame(key) OVER %[4]s AND %[3]s = lagInFrame(%[3]s) OVER %[4]s) AS brk
	      FROM %[1]s)))
	  GROUP BY batch_id, parent)`, numbered(s), render, val, wAll)
}

func dblSQL(d string) string {
	if d == "NULL" {
		return "''"
	}
	return fmt.Sprintf("multiIf(isNaN(%[1]s), 'NaN', isInfinite(%[1]s), if(%[1]s > 0, 'Infinity', '-Infinity'), "+
		"abs(%[1]s) >= 1e21 OR (%[1]s != 0 AND abs(%[1]s) < 1e-6), replaceRegexpOne(toString(%[1]s), 'e([0-9])', 'e+\\\\1'), toString(%[1]s))", d)
}

// rawChildren decodes events or links: id is delta over non-null values,
// parent_id quasi-delta on the equality column (event name, link trace_id).
func rawChildren(s RawSource, eq string, extra string) string {
	return fmt.Sprintf(`(SELECT *, if(id IS NULL, NULL, sum(ifNull(id, 0)) OVER %[3]s) AS cid,
	    sum(toUInt64(parent_id)) OVER (PARTITION BY batch_id, run ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS parent
	  FROM (SELECT *, sum(brk) OVER %[3]s AS run FROM (
	    SELECT batch_id, rn, id, parent_id, %[4]s,
	      NOT (row_number() OVER %[3]s > 1 AND %[2]s = lagInFrame(%[2]s) OVER %[3]s) AS brk FROM %[1]s)))`,
		numbered(s), eq, wAll, extra)
}

// RawTracesSelect rebuilds otel_traces rows from raw OTAP payloads.
func RawTracesSelect(src map[string]RawSource, env RawEnvelope) string {
	sp := src["spans"]
	ev, lk := src["span_events"], src["span_links"]
	evSQL := "(SELECT toUInt64(0) AS batch_id, toUInt64(0) AS parent_id, [toDateTime64(0, 9)] AS ts, [''] AS nm, [map('', '')] AS at WHERE 0)"
	if ev.From != "" {
		evSQL = fmt.Sprintf(`(SELECT e.batch_id AS batch_id, e.parent AS parent_id, groupArray(ifNull(e.time_unix_nano, toDateTime64(0, 9))) AS ts,
	  groupArray(toString(e.name)) AS nm, groupArray(ea.m) AS at
	  FROM %s AS e LEFT JOIN ea ON ea.batch_id = e.batch_id AND ea.parent = e.cid GROUP BY e.batch_id, e.parent)`,
			rawChildren(ev, "name", fmt.Sprintf("%s AS time_unix_nano, name", ev.col("time_unix_nano"))))
	}
	lkSQL := "(SELECT toUInt64(0) AS batch_id, toUInt64(0) AS parent_id, [''] AS tid, [''] AS sid, [''] AS st, [map('', '')] AS at WHERE 0)"
	if lk.From != "" {
		lkSQL = fmt.Sprintf(`(SELECT l.batch_id AS batch_id, l.parent AS parent_id, groupArray(%s) AS tid, groupArray(%s) AS sid,
	  groupArray(ifNull(toString(l.trace_state), '')) AS st, groupArray(la.m) AS at
	  FROM %s AS l LEFT JOIN la ON la.batch_id = l.batch_id AND la.parent = l.cid GROUP BY l.batch_id, l.parent)`,
			hexOrEmpty("l.trace_id"), hexOrEmpty("l.span_id"),
			rawChildren(lk, "trace_id", fmt.Sprintf("trace_id, span_id, %s AS trace_state", lk.col("trace_state"))))
	}
	spans := fmt.Sprintf(`(SELECT *, rn - min(rn) OVER (PARTITION BY batch_id) AS rn0,
	    if(id IS NULL, NULL, sum(ifNull(id, 0)) OVER %[2]s) AS sid_abs,
	    sum(ifNull(tupleElement(resource, 'id', NULL), 0)) OVER %[2]s AS rid_abs
	  FROM %[1]s)`, numbered(sp), wAll)
	return strings.NewReplacer("\n\t", " ").Replace(fmt.Sprintf(`WITH
	ra AS %s,
	sa AS %s,
	ea AS %s,
	la AS %s,
	ev AS %s,
	lk AS %s
	SELECT s.start_time_unix_nano AS Timestamp, %s AS TraceId, %s AS SpanId, %s AS ParentSpanId,
	  ifNull(toString(%s), '') AS TraceState, toString(s.name) AS SpanName,
	  transform(ifNull(%s, 0), [0, 1, 2, 3, 4, 5], ['Unspecified', 'Internal', 'Server', 'Client', 'Producer', 'Consumer'], '') AS SpanKind,
	  ra.m['service.name'] AS ServiceName, ra.m AS ResourceAttributes,
	  ifNull(toString(tupleElement(s.scope, 'name', NULL)), '') AS ScopeName, ifNull(toString(tupleElement(s.scope, 'version', NULL)), '') AS ScopeVersion,
	  sa.m AS SpanAttributes, toUInt64(toInt64(s.duration_time_unix_nano)) AS Duration,
	  transform(ifNull(tupleElement(s.status, 'code', NULL), 0), [0, 1, 2], ['Unset', 'Ok', 'Error'], '') AS StatusCode,
	  ifNull(toString(tupleElement(s.status, 'status_message', NULL)), '') AS StatusMessage,
	  ev.ts AS `+"`Events.Timestamp`"+`, ev.nm AS `+"`Events.Name`"+`, ev.at AS `+"`Events.Attributes`"+`,
	  lk.tid AS `+"`Links.TraceId`"+`, lk.sid AS `+"`Links.SpanId`"+`, lk.st AS `+"`Links.TraceState`"+`, lk.at AS `+"`Links.Attributes`"+`,
	  %s
	FROM %s AS s
	LEFT JOIN ra ON ra.batch_id = s.batch_id AND ra.parent = s.rid_abs
	LEFT JOIN sa ON sa.batch_id = s.batch_id AND sa.parent = s.sid_abs
	LEFT JOIN ev ON ev.batch_id = s.batch_id AND ev.parent_id = s.sid_abs
	LEFT JOIN lk ON lk.batch_id = s.batch_id AND lk.parent_id = s.sid_abs`,
		rawAttrs(src["resource_attrs"]), rawAttrs(src["span_attrs"]), rawAttrs(src["span_event_attrs"]), rawAttrs(src["span_link_attrs"]),
		evSQL, lkSQL,
		hexOrEmpty("s.trace_id"), hexOrEmpty("s.span_id"), hexOrEmpty("s.parent_span_id"),
		sp.qcol("s", "trace_state"), sp.qcol("s", "kind"),
		strings.ReplaceAll(env.sql("s.batch_id"), "rn0", "s.rn0"), spans))
}

// RawLogsSelect rebuilds otel_logs rows from raw OTAP payloads. Map and
// slice bodies are CBOR, which SQL cannot render: they come out as ”.
func RawLogsSelect(src map[string]RawSource, env RawEnvelope) string {
	lg := src["logs"]
	logs := fmt.Sprintf(`(SELECT *, rn - min(rn) OVER (PARTITION BY batch_id) AS rn0,
	    if(id IS NULL, NULL, sum(ifNull(id, 0)) OVER %[2]s) AS lid_abs,
	    sum(ifNull(tupleElement(resource, 'id', NULL), 0)) OVER %[2]s AS rid_abs,
	    sum(ifNull(tupleElement(scope, 'id', NULL), 0)) OVER %[2]s AS scid_abs
	  FROM %[1]s)`, numbered(lg), wAll)
	b := func(f string) string { return fmt.Sprintf("tupleElement(g.body, '%s', NULL)", f) }
	body := fmt.Sprintf("multiIf(%[1]s = 1, ifNull(toString(%[2]s), ''), %[1]s = 2, toString(%[3]s), %[1]s = 3, %[4]s, %[1]s = 4, if(%[5]s, 'true', 'false'), %[1]s = 7, base64Encode(ifNull(toString(%[6]s), '')), '')",
		b("type"), b("str"), b("int"), dblSQL(b("double")), b("bool"), b("bytes"))
	return strings.NewReplacer("\n\t", " ").Replace(fmt.Sprintf(`WITH
	ra AS %s,
	sc AS %s,
	la AS %s
	SELECT if(toUnixTimestamp64Nano(ifNull(%[4]s, toDateTime64(0, 9))) = 0, ifNull(%[5]s, toDateTime64(0, 9)), %[4]s) AS Timestamp,
	  %[6]s AS TraceId, %[7]s AS SpanId, toUInt8(ifNull(%[8]s, 0) %% 256) AS TraceFlags,
	  ifNull(toString(%[9]s), '') AS SeverityText, toUInt8(ifNull(%[10]s, 0) %% 256) AS SeverityNumber,
	  ra.m['service.name'] AS ServiceName, if(g.body IS NULL, '', %[11]s) AS Body,
	  ifNull(toString(tupleElement(g.resource, 'schema_url', NULL)), '') AS ResourceSchemaUrl, ra.m AS ResourceAttributes,
	  ifNull(toString(%[12]s), '') AS ScopeSchemaUrl, ifNull(toString(tupleElement(g.scope, 'name', NULL)), '') AS ScopeName,
	  ifNull(toString(tupleElement(g.scope, 'version', NULL)), '') AS ScopeVersion,
	  sc.m AS ScopeAttributes, la.m AS LogAttributes, ifNull(toString(%[13]s), '') AS EventName,
	  %[14]s
	FROM %[15]s AS g
	LEFT JOIN ra ON ra.batch_id = g.batch_id AND ra.parent = g.rid_abs
	LEFT JOIN sc ON sc.batch_id = g.batch_id AND sc.parent = g.scid_abs
	LEFT JOIN la ON la.batch_id = g.batch_id AND la.parent = g.lid_abs`,
		rawAttrs(src["resource_attrs"]), rawAttrs(src["scope_attrs"]), rawAttrs(src["log_attrs"]),
		lg.qcol("g", "time_unix_nano"), lg.qcol("g", "observed_time_unix_nano"),
		hexOrEmpty(lg.qcol("g", "trace_id")), hexOrEmpty(lg.qcol("g", "span_id")), lg.qcol("g", "flags"),
		lg.qcol("g", "severity_text"), lg.qcol("g", "severity_number"), body, lg.qcol("g", "schema_url"), lg.qcol("g", "event_name"),
		strings.ReplaceAll(env.sql("g.batch_id"), "rn0", "g.rn0"), logs))
}
