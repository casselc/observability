#!/bin/bash
# Data for the sorting experiment (not measured; run only between measured
# blocks). mixgen (gen/main.go) batches as publisher 0 of 3 behind a cluster
# gateway without routing: 120 services, Zipf 1.1, 3,000 pods on 200 nodes,
# 10k rows per batch; traces at 30k spans/s and logs at 10k logs/s for the
# cluster (the mid scenario's 600k/s and 200k/s over 20 clusters), so a
# traces batch spans 1 s and a logs batch 3 s.
#   prep.sh data     32 traces + 32 logs batches -> $S/sorting/data/
#   prep.sh route    step 2: for N = 3, 8, 16 publishers with routing_key
#                    service, the publishers that own the query services
#                    (services.json): 8 batches per signal each, encoded with
#                    configs a, b, d, f to otel/sorting/route/N<N>p<j>-<cfg>/,
#                    then the .pb files are deleted (disk).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
G=$S/sorting/bin/mixgen
EB=$S/sorting/bin/sort/encbench
mkdir -p $here/data
case ${1:-data} in
data)
  for sig in traces logs; do
    rate=30000; [ $sig = logs ] && rate=10000
    [ -f $S/sorting/data/$sig/$sig-b0031.pb ] && continue
    $G -signal $sig -out $S/sorting/data/$sig -batches 32 -rate $rate -publishers 3 -route none -pub 0 \
      -stats $here/data/$sig-stats.jsonl
  done;;
route)
  declare -A FL=([a-unsorted]="" [b-sorted-1rg]="--sort service_time --row-groups 1"
    [d-hash-16rg]="--sort service_time --row-groups 16 --split hash" [f-range-16rg]="--sort service_time --row-groups 16 --split range")
  for N in 3 8 16; do
    $G -ring -publishers $N > $here/data/ring-N$N.json
    pubs=$(python3 -c "
import json; r = json.load(open('$here/data/ring-N$N.json')); s = json.load(open('$here/services.json'))
print(' '.join(sorted({str(r[v['service']]['publisher']) for v in s.values()})))")
    for j in $pubs; do
      tag=N${N}p$j
      grep -q "\"tag\": \"$tag\"" $here/data/route-done.jsonl 2>/dev/null && continue
      for sig in traces logs; do
        rate=30000; [ $sig = logs ] && rate=10000
        $G -signal $sig -out $S/sorting/rdata/$tag/$sig -batches 8 -rate $rate -publishers $N -route service -pub $j \
          -stats $here/data/route-$tag-$sig-stats.jsonl
        for c in a-unsorted b-sorted-1rg d-hash-16rg f-range-16rg; do
          $EB --files "$(ls $S/sorting/rdata/$tag/$sig/*.pb | paste -sd,)" --signal $sig --warmup 1 --batches 7 ${FL[$c]} --label $c \
            --s3 http://127.0.0.1:18333/otel/sorting/route/$tag/$c/edges/sort-$c --key otel --secret otelsecret \
            | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); d.update(tag='$tag', signal='$sig', config='$c'); print(json.dumps(d))" >> $here/data/route-edge.jsonl
        done
      done
      rm -rf $S/sorting/rdata/$tag
      echo "{\"tag\": \"$tag\"}" >> $here/data/route-done.jsonl
    done
  done;;
esac
