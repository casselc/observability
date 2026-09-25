#!/bin/bash
# bench_s3.sh VARIANT: S3 requests and CPU of one lifecycle, zero-copy against
# plain replication (a copy per replica), on both replicas.
#   VARIANT zc:  storage_policy tiered_zc,  allow_remote_fs_zero_copy_replication = 1
#   VARIANT own: storage_policy tiered_own, allow_remote_fs_zero_copy_replication = 0
# Lifecycle (otel_traces schema, zx.b_<variant>):
#   insert  N parts of R rows on r1, hot volume; r2 fetches them (local disk)
#   move    TTL moves every part to the S3 volume (both replicas)
#   merge   OPTIMIZE FINAL: the parts merged on the S3 volume
#   clean   the merged-away parts removed (old_parts_lifetime)
#   drop    DROP TABLE on both replicas
# (Merges are held off with max_bytes_to_merge_* = 1 until the merge phase.)
# Prints per phase and replica the system.events deltas and bytes in the bucket.
set -u
. /home/user/oscope/spike/otel-chdb/central-replicated/scripts/common.sh
. "$HERE/scripts/gen.sh"
V=$1; N=${N:-10}; ROWS=${ROWS:-30000}
case $V in zc) POL=tiered_zc; ZC=1; PFX=zc ;; own) POL=tiered_own; ZC=0; PFX='own-r1 own-r2' ;; esac
T=zx.b_$V
EV="name IN ('S3PutObject','S3UploadPart','S3CreateMultipartUpload','S3GetObject','S3HeadObject','S3ListObjects','S3DeleteObjects','S3CopyObject','ZooKeeperTransactions','UserTimeMicroseconds','SystemTimeMicroseconds')"
snap() { for r in 1 2; do u=$([ $r = 1 ] && echo $R1 || echo $R2); curl -sS "$u/" --data-binary "SELECT 'r$r', name, value FROM system.events WHERE $EV FORMAT TSV"; done > /tmp/.bench.$V.$1; }
diffp() {
  python3 - /tmp/.bench.$V.$1 /tmp/.bench.$V.$2 "$3" "$(s3du $PFX | awk '{o+=$2; b+=$3} END {print o, b}')" <<'EOF'
import sys
def rd(f):
    d = {}
    for l in open(f):
        r, n, v = l.rstrip("\n").split("\t"); d[(r, n)] = int(v)
    return d
a, b = rd(sys.argv[1]), rd(sys.argv[2])
for r in ("r1", "r2"):
    g = lambda k: b.get((r, k), 0) - a.get((r, k), 0)
    put = g("S3PutObject") + g("S3UploadPart")
    cpu = (g("UserTimeMicroseconds") + g("SystemTimeMicroseconds")) / 1e6
    print(f"{sys.argv[3]}\t{r}\tPUT {put}\tGET {g('S3GetObject')}\tHEAD {g('S3HeadObject')}\tLIST {g('S3ListObjects')}\tDELETE {g('S3DeleteObjects')}\tkeeper {g('ZooKeeperTransactions')}\tcpu_s {cpu:.2f}\tbucket objects/bytes {sys.argv[4]}")
EOF
}
wait_q() { # until no queue/moves/merges for T and $1 holds
  for _ in $(seq 1 150); do
    n=$(for u in $R1 $R2; do curl -sS "$u/" --data-binary "SELECT (SELECT count() FROM system.replication_queue WHERE database='zx' AND table='b_$V') + (SELECT count() FROM system.moves WHERE database='zx' AND table='b_$V') + (SELECT count() FROM system.merges WHERE database='zx' AND table='b_$V') + ($1)"; done | awk '{s+=$1} END {print s}')
    [ "$n" = 0 ] && return; sleep 2
  done; echo "timeout waiting: $1"
}
python3 "$HERE/scripts/ddl.py" --db zx --signals traces --suffix _b --policy $POL --zero-copy $ZC --move '30 SECOND' --delete '1 DAY' \
  --extra 'old_parts_lifetime = 5, cleanup_delay_period = 1, max_cleanup_delay_period = 2, cleanup_delay_period_random_add = 0, max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1' \
  | grep -v 'CREATE DATABASE' | sed "s/otel_traces_b/b_$V/" > /tmp/.bench.$V.sql
apply_sql 29000 /tmp/.bench.$V.sql && apply_sql 39000 /tmp/.bench.$V.sql
snap 0
for i in $(seq 1 $N); do gen_traces $R1 $T $ROWS b$V$i; done
wait_q "(SELECT count() FROM system.parts WHERE database='zx' AND table='b_$V' AND active) != $N"
snap 1; diffp 0 1 insert
wait_q "(SELECT count() FROM system.parts WHERE database='zx' AND table='b_$V' AND active AND disk_name = 'default')"
snap 2; diffp 1 2 move
q1 "ALTER TABLE $T MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 161061273600, max_bytes_to_merge_at_min_space_in_pool = 1048576 SETTINGS alter_sync = 2"; q1 "OPTIMIZE TABLE $T FINAL SETTINGS alter_sync = 2"
wait_q "(SELECT count() FROM system.parts WHERE database='zx' AND table='b_$V' AND active) != 1"
snap 3; diffp 2 3 merge
wait_q "(SELECT count() FROM system.parts WHERE database='zx' AND table='b_$V' AND NOT active)"
snap 4; diffp 3 4 clean
q2 "DROP TABLE $T SYNC"; q1 "DROP TABLE $T SYNC"; sleep 3
snap 5; diffp 4 5 drop
