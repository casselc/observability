#!/bin/sh
# fixedcost.sh RUN: where the ~16 ms per insert goes, and whether grouping
# objects into one INSERT amortizes it. Uses the gauge objects a
# `load -keep -run RUN -pods-per-batch 40` left in S3 (5,600 points each) and
# the tables it created in ml_fixed. Prints costprobe lines.
RUN=$1
P=${ML_COSTPROBE:-costprobe}
S3="http://127.0.0.1:18333/otel/metrics-layout/load/$RUN"
cred="'otel', 'otelsecret'"
BST="MetricName String, ServiceName String, series_id UInt64, StartTimeUnix DateTime, TimeUnix DateTime, Value Float64, Flags UInt32, \`Exemplars.TimeUnix\` Array(DateTime), \`Exemplars.Value\` Array(Float64), \`Exemplars.SpanId\` Array(String), \`Exemplars.TraceId\` Array(String), producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
ONE="max_threads = 1, max_insert_threads = 1, max_block_size = 1048576, max_insert_block_size = 1048576, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, input_format_parquet_max_block_size = 1048576, input_format_parquet_prefer_block_bytes = 17179869184"
$P -db ml_fixed \
 "CREATE TABLE ml_fixed.g_one AS ml_fixed.otel_metrics_gauge_points" \
 "CREATE TABLE ml_fixed.g_many AS ml_fixed.otel_metrics_gauge_points" \
 "CREATE TABLE ml_fixed.g_null AS ml_fixed.otel_metrics_gauge_points ENGINE = Null" \
 "CREATE TABLE ml_fixed.a_one AS ml_fixed.otel_metrics_gauge" \
 "CREATE TABLE ml_fixed.a_many AS ml_fixed.otel_metrics_gauge"
for b in 10 11 12 13 14; do
  o=$(printf '%08d' $b)
  $P -db ml_fixed \
   "SELECT count() FROM s3('$S3/B/gauge/$o.parquet', $cred, 'Parquet', '$BST')" \
   "SELECT sum(Value) FROM s3('$S3/B/gauge/$o.parquet', $cred, 'Parquet', '$BST') SETTINGS max_threads = 1" \
   "INSERT INTO ml_fixed.g_null SETTINGS $ONE SELECT * FROM s3('$S3/B/gauge/$o.parquet', $cred, 'Parquet', '$BST')" \
   "INSERT INTO ml_fixed.g_one SETTINGS $ONE SELECT * FROM s3('$S3/B/gauge/$o.parquet', $cred, 'Parquet', '$BST')" \
   "INSERT INTO ml_fixed.a_one SELECT * EXCEPT (producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version) FROM s3('$S3/A/gauge/$o.parquet', $cred, 'Parquet') SETTINGS $ONE"
done
$P -db ml_fixed \
 "INSERT INTO ml_fixed.g_many SETTINGS $ONE SELECT * FROM s3('$S3/B/gauge/{00000020..00000029}.parquet', $cred, 'Parquet', '$BST')" \
 "INSERT INTO ml_fixed.g_many SETTINGS $ONE SELECT * FROM s3('$S3/B/gauge/{00000030..00000039}.parquet', $cred, 'Parquet', '$BST')" \
 "INSERT INTO ml_fixed.a_many SELECT * EXCEPT (producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version) FROM s3('$S3/A/gauge/{00000020..00000029}.parquet', $cred, 'Parquet') SETTINGS $ONE" \
 "INSERT INTO ml_fixed.a_many SELECT * EXCEPT (producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version) FROM s3('$S3/A/gauge/{00000030..00000039}.parquet', $cred, 'Parquet') SETTINGS $ONE"
