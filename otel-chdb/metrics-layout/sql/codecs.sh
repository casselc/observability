#!/bin/sh
# codecs.sh DB: copies B's points columns into one table per codec variant
# (same sort key, so the same order) and prints compressed bytes per point
# per column and variant, after OPTIMIZE FINAL. Used to pick b_tables.sql's
# {valuecodec} and {bucketcodec}; see README, "Codecs".
CH=${ML_CLICKHOUSE:-http://127.0.0.1:18123}
DB=$1
# ALP is beta in 26.10 (enable_alp_codec); the TimeSeries engine uses it.
q() { curl -sS --fail-with-body "$CH/?enable_alp_codec=1" --data-binary "$1"; }
KEY="MetricName LowCardinality(String), ServiceName LowCardinality(String), series_id UInt64, TimeUnix DateTime"
VARIANTS=${VARIANTS:-"zstd1:ZSTD(1);zstd3:ZSTD(3);gorilla:Gorilla, ZSTD(1);fpc:FPC, ZSTD(1);alp:ALP, ZSTD(1);ddelta:DoubleDelta, ZSTD(1);delta:Delta, ZSTD(1);t64:T64, ZSTD(1)"}
IFS=';'
set -f
for v in $VARIANTS; do
  IFS=' '
  name=${v%%:*}; codec=${v#*:}
  for tc in "gauge:Value:Float64" "sum:Value:Float64" "histogram:Sum:Float64" "histogram:BucketCounts:Array(UInt64)" "histogram:Count:UInt64" "histogram:Max:Float64"; do
    t=${tc%%:*}; rest=${tc#*:}; c=${rest%%:*}; typ=${rest#*:}
    tbl="codec_${name}_${t}_$c"
    if q "CREATE TABLE $DB.$tbl ($KEY, $c $typ CODEC($codec)) ENGINE = MergeTree ORDER BY (MetricName, ServiceName, series_id, TimeUnix)" >/dev/null 2>&1; then
      q "INSERT INTO $DB.$tbl SELECT MetricName, ServiceName, series_id, TimeUnix, $c FROM $DB.otel_metrics_${t}_points"
      q "OPTIMIZE TABLE $DB.$tbl FINAL"
      q "SELECT '$name', '$t', '$c', round(sum(data_compressed_bytes) / (SELECT count() FROM $DB.$tbl), 3) FROM system.columns WHERE database = '$DB' AND table = '$tbl' AND name = '$c' FORMAT TSV"
      q "DROP TABLE $DB.$tbl"
    else
      echo "$name	$t	$c	n/a"
    fi
  done
done
