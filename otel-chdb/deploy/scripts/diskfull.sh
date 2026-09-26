#!/bin/bash
# The Rust publisher's durable buffer when it fills (DECISIONS.md risk #14):
# what the client is told, and what survives, for
#   cap-backpressure  retention_size_cap 200 MiB, size_cap_policy: backpressure
#   cap-drop_oldest   retention_size_cap 200 MiB, size_cap_policy: drop_oldest
#                     (the engine refuses a cap under 192 MiB per core: WAL
#                     max 128 MiB + 2 segments of 32 MiB)
#   enospc            retention_size_cap 1 GiB (the deployed shape: cap below
#                     the volume) but the buffer on a 160 MiB tmpfs, so the
#                     filesystem fills first
# Each case: S3 unreachable (a closed port); a sender streams 32 distinct
# 10k-span requests (~6 MB of OTLP each) and, concurrently, 32 distinct
# 10k-log requests (~4.5 MB), resending on 503, for at most 60 s; the publisher is
# SIGKILLed; it restarts on the same buffer with S3 up, drains for 25 s and
# stops. Then every committed row is mapped back to its request (mixgen
# batch i covers [start + i*window, start + (i+1)*window)).
#
#   BIN=.../otap-s3pq SEND=.../otlpsend MIX=... W=workdir TMPFS=a-160MiB-tmpfs-mountpoint diskfull.sh
set -u
here=$(cd "$(dirname "$0")/../.." && pwd)
: "${BIN:?}" "${SEND:?}" "${MIX:?}" "${W:?}" "${TMPFS:?}"
S3=${S3:-http://127.0.0.1:18333}; CH=${CH:-http://127.0.0.1:18123}; RUN=${RUN:-df$(date +%s)}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-otel} AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-otelsecret}
q() { curl -s "$CH/" --data-binary "$1"; }
mkdir -p "$W"
S0=1790416800000000000; WIN=333333333
out=$W/summary.txt; : > "$out"
declare -A acked committed rows before
edge() { # case s3url bufdir cap policy log -> pid
  sed "s/size_cap_policy: backpressure/size_cap_policy: $5/" "$here/otap-rs/configs/edge-durable.yaml" > "$W/$1.yaml"
  setsid bash -c 'echo $$ > "$0"; exec "$@"' "$W/$1.pid" env MEM_SOURCE=rss OTLP_HTTP=127.0.0.1:14718 OTLP_GRPC=127.0.0.1:14717 \
    ADMIN_HTTP=127.0.0.1:14719 PRODUCER=diskfull BUFFER_DIR="$3" BUFFER_CAP="$4" PUT_TIMEOUT=2s VERBOSE=true S3_URL="$2" \
    "$BIN" -c "$W/$1.yaml" >> "$6" 2>&1 < /dev/null &
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:14718/ && break; sleep 0.1; done
}
for c in ${CASES:-cap-backpressure:200MiB:backpressure cap-drop_oldest:200MiB:drop_oldest enospc:1GiB:backpressure}; do
  IFS=: read -r name cap policy <<< "$c"
  if [ "$name" = enospc ]; then buf=$TMPFS/buf; else buf=$W/$name-buf; fi
  rm -rf "$buf"; mkdir -p "$buf"
  url=$S3/deploy-edge/diskfull/$RUN/$name
  edge "$name" "http://127.0.0.1:18399/deploy-edge/diskfull/$RUN/$name" "$buf" "$cap" "$policy" "$W/$name.edge.log"
  senders=""
  for sig in traces logs; do
    timeout 60 "$SEND" -url http://127.0.0.1:14718 -signal $sig -file "$(ls "$MIX"/$sig-b00*.pb | paste -sd,)" -n 32 \
      -backoff 500ms -timeout 20s > "$W/$name.send-$sig.jsonl" 2> "$W/$name.send-$sig.err" &
    senders="$senders $!"
  done
  wait $senders
  du=$(du -sm "$buf" | cut -f1)
  kill -KILL "$(cat "$W/$name.pid")"; sleep 1
  edge "$name" "$url" "$buf" "$cap" "$policy" "$W/$name.edge2.log"
  sleep 25
  kill -INT "$(cat "$W/$name.pid")" 2>/dev/null; sleep 3
  recovered=""
  if [ "$name" = enospc ]; then
    # The filesystem is still full at the restart. Grow it (what expanding
    # the PVC does) and start once more: is the acked data stuck or lost?
    for sig in traces logs; do before[$sig]=$(q "SELECT count() FROM s3('$url/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'Timestamp DateTime64(9)') FORMAT TSV" 2>/dev/null); done
    mount -o remount,size=512m "$TMPFS" && recovered="after growing the filesystem to 512 MiB and a third start; before that: traces ${before[traces]:-0} rows, logs ${before[logs]:-0} rows"
    edge "$name" "$url" "$buf" "$cap" "$policy" "$W/$name.edge3.log"
    sleep 25
    kill -INT "$(cat "$W/$name.pid")" 2>/dev/null; sleep 3
    mount -o remount,size=160m "$TMPFS"
  fi
  for sig in traces logs; do
    acked[$sig]=$(grep -c '"seq"' "$W/$name.send-$sig.jsonl" 2>/dev/null || echo 0)
    committed[$sig]=$(q "SELECT arrayStringConcat(arraySort(groupArray(b)), ',') FROM (SELECT intDiv(toUnixTimestamp64Nano(Timestamp) - $S0, $WIN) b, count() c FROM s3('$url/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'Timestamp DateTime64(9)') GROUP BY b HAVING c = 10000) FORMAT TSV" 2>/dev/null)
    rows[$sig]=$(q "SELECT count() FROM s3('$url/$sig/**.parquet', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY', 'Parquet', 'Timestamp DateTime64(9)') FORMAT TSV" 2>/dev/null)
  done
  errs=$(cat "$W/$name".send-*.err | grep -o 'attempt [0-9]*: .*' | sed 's/attempt [0-9]*: //; s/request [0-9]* //' | cut -c1-90 | sort | uniq -c | sort -rn | head -3 | sed 's/^ */    /')
  logs=$(grep -hoE 'durable_buffer\.[a-z_.]+|No space left on device|panicked|out of space|ENOSPC' "$W/$name".edge*.log | sort | uniq -c | sort -rn | head -12 | sed 's/^ */    /')
  {
    echo "## $name: retention_size_cap $cap, size_cap_policy $policy, buffer on $( [ "$name" = enospc ] && echo '160 MiB tmpfs' || echo 'the scratch disk')"
    echo "buffer directory after the send phase: ${du} MB"
    [ -n "$recovered" ] && echo "counts below are $recovered"
    echo "sender errors (count, first 90 chars):"; echo "$errs"
    for sig in traces logs; do
      echo "$sig: acked before the SIGKILL ${acked[$sig]} of 32; committed whole after the restart: [${committed[$sig]}]; rows in S3 ${rows[$sig]:-0}"
      echo "  acked but not committed (lost): $(python3 -c "
a=set(range(${acked[$sig]})); c=set(int(x) for x in '${committed[$sig]}'.split(',') if x)
print(sorted(a-c) or 'none', '; committed but never acked:', sorted(c-a) or 'none')")"
    done
    echo "publisher log markers (count):"; echo "$logs"
    echo
  } >> "$out"
  rm -rf "$buf"
done
cat "$out"
