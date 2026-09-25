# Sourced by the scripts here.
SP=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
CHBIN=$SP/ch/clickhouse
HERE=/home/user/oscope/spike/otel-chdb/central-replicated
R1=http://127.0.0.1:28123
R2=http://127.0.0.1:38123
q1() { curl -sS "$R1/" --data-binary "$1"; }
q2() { curl -sS "$R2/" --data-binary "$1"; }
qb() { echo "r1: $(q1 "$1")"; echo "r2: $(q2 "$1")"; }
# Apply a multi-statement SQL file to a replica by TCP port (29000 / 39000).
apply_sql() { $CHBIN client --port "$1" --multiquery < "$2"; }
# S3 usage of a prefix of the central-zc bucket: objects and bytes (S3 ListObjectsV2).
s3du() { python3 $HERE/scripts/s3du.py "$@"; }
