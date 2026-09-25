# gen_traces URL TABLE ROWS KEY: one INSERT of ROWS synthetic spans (one part), content_key KEY.
gen_traces() {
  curl -sS "$1/" --data-binary "INSERT INTO $2 (Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes, Duration, StatusCode, StatusMessage, producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version, content_key)
SELECT fromUnixTimestamp64Nano(toInt64(toUnixTimestamp64Nano(now64(9)) - number * 1000000)), hex(cityHash64(number, '$4')), hex(cityHash64(number, 1, '$4')), hex(cityHash64(number, 2, '$4')), '',
  concat('op-', toString(number % 37)), 'SPAN_KIND_SERVER', concat('svc-', toString(number % 11)),
  map('k8s.pod.name', concat('pod-', toString(number % 97)), 'host.name', concat('node-', toString(number % 13)), 'service.version', '1.2.3'),
  'scope', '1.0', map('http.route', concat('/api/v1/', toString(number % 53)), 'http.status_code', toString(200 + number % 5), 'req', toString(rand())),
  rand() % 1000000, 'STATUS_CODE_OK', '', 'gen', '$4', 0, toUInt32(number), now64(9), 1, '$4'
FROM numbers($3) SETTINGS max_block_size = 10000000, max_insert_block_size = 10000000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0"
}
