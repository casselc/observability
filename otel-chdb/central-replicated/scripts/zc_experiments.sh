#!/bin/bash
# zc_experiments.sh: zero-copy mechanics, side by side with plain replicated S3.
#
# Two tables with the consumer's otel_traces schema, one per S3 disk:
#   zx.t_zc   storage_policy tiered_zc  (s3_zc: both replicas share one prefix), zero-copy ON
#   zx.t_own  storage_policy tiered_own (s3_own: a prefix per replica),         zero-copy OFF
# Every step prints the S3 usage of each prefix, the blob check (orphans /
# missing) and per-replica system.events deltas (S3 requests, Keeper), and
# appends part_log rows to part_log.tsv.
#
#   . scripts/zc_experiments.sh   (a library of steps; the README lists the sequence run)
set -u
. /home/user/oscope/spike/otel-chdb/central-replicated/scripts/common.sh
. "$HERE/scripts/gen.sh"
OUT=${OUT:-$HERE/results/zc}
mkdir -p "$OUT"
log() { echo "$(date +%T) $*" | tee -a "$OUT/log.txt"; }
EV="name LIKE 'S3%' OR name LIKE 'DiskS3%' OR name IN ('ZooKeeperTransactions','ZooKeeperMulti','ZooKeeperCreate','ZooKeeperRemove','ZooKeeperList','ZooKeeperGet','ZooKeeperExists','ReplicatedPartFetches','ReplicatedPartMerges','UserTimeMicroseconds','SystemTimeMicroseconds')"
evsnap() { for r in 1 2; do u=$([ $r = 1 ] && echo $R1 || echo $R2); curl -sS "$u/" --data-binary "SELECT 'r$r', name, value FROM system.events WHERE $EV FORMAT TSV" 2>/dev/null; done > "$OUT/.ev.$1"; }
evdiff() { # a b: nonzero deltas, per replica
  python3 - "$OUT/.ev.$1" "$OUT/.ev.$2" <<'EOF'
import sys, collections
def rd(f):
    d = {}
    for l in open(f):
        r, n, v = l.rstrip("\n").split("\t"); d[(r, n)] = int(v)
    return d
a, b = rd(sys.argv[1]), rd(sys.argv[2])
keep = ["S3PutObject", "S3GetObject", "S3HeadObject", "S3ListObjects", "S3DeleteObjects", "S3CopyObject", "S3CreateMultipartUpload",
        "S3UploadPart", "S3CompleteMultipartUpload", "ZooKeeperTransactions", "ReplicatedPartFetches", "ReplicatedPartMerges"]
for r in ("r1", "r2"):
    print(f"  {r}: " + ", ".join(f"{k} {b.get((r, k), 0) - a.get((r, k), 0)}" for k in keep if b.get((r, k), 0) - a.get((r, k), 0)))
EOF
}
state() { # label
  log "--- $1"
  for t in t_zc t_own; do
    qb "SELECT '$t', disk_name, count() parts, sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = 'zx' AND table = '$t' AND active GROUP BY disk_name ORDER BY disk_name FORMAT TSV" | tee -a "$OUT/log.txt"
  done
  s3du zc own-r1 own-r2 | sed 's/^/  s3 /' | tee -a "$OUT/log.txt"
  python3 "$HERE/scripts/blobcheck.py" s3_zc zc | python3 -c "import json,sys; d=json.load(sys.stdin); [d.pop(k) for k in ('missing_examples','by_db_kind')]; print(json.dumps(d))" | sed 's/^/  zc  /' | tee -a "$OUT/log.txt"
  python3 "$HERE/scripts/blobcheck.py" s3_own "own-{r}" | python3 -c "import json,sys; d=json.load(sys.stdin); [d.pop(k) for k in ('missing_examples','by_db_kind')]; print(json.dumps(d))" | sed 's/^/  own /' | tee -a "$OUT/log.txt"
}
partlog() { # every part_log row of database zx on both replicas, with its CPU and S3 requests
  for r in 1 2; do u=$([ $r = 1 ] && echo $R1 || echo $R2)
    curl -sS "$u/" --data-binary "SYSTEM FLUSH LOGS" >/dev/null
    curl -sS "$u/" --data-binary "SELECT event_time_microseconds, 'r$r', table, event_type, merge_reason, disk_name, part_name, rows, size_in_bytes, duration_ms,
      ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] cpu_us,
      ProfileEvents['S3PutObject'] + ProfileEvents['S3UploadPart'] put, ProfileEvents['S3GetObject'] get, ProfileEvents['S3CopyObject'] copy,
      ProfileEvents['WriteBufferFromS3Bytes'] s3_written, error
      FROM system.part_log WHERE database = 'zx' ORDER BY event_time_microseconds FORMAT TSVWithNames"
  done > "$OUT/part_log.tsv"
}
settle() { # wait for replication queues and moves to drain
  for _ in $(seq 1 120); do
    n=$(for u in $R1 $R2; do curl -sS "$u/" --data-binary "SELECT (SELECT count() FROM system.replication_queue WHERE database='zx') + (SELECT count() FROM system.moves WHERE database='zx') + (SELECT count() FROM system.merges WHERE database='zx')" 2>/dev/null || echo 1; done | awk "{s+=\$1} END{print s}")
    [ "$n" = 0 ] && break; sleep 2
  done
}
