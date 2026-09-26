#!/bin/bash
# Restart-safe orchestration of the sorting experiment. After a container
# restart:  cd spike/otel-chdb/bench/sorting && setsid nohup bash driver.sh >> driver.log 2>&1 &
# Every step skips what it has already done; a step's marker in state/ means
# it finished. Measured steps never overlap anything else this driver runs.
set -u
here=$(cd "$(dirname "$0")" && pwd); spike=$(dirname "$(dirname "$here")")
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
export CARGO_TARGET_DIR=$S/otap-rs-target CARGO_BUILD_JOBS=4 PATH=$HOME/.cargo/bin:$PATH
mkdir -p $here/state
log() { echo "$(date -u +%FT%TZ) $*"; }
step() { [ -f $here/state/$1 ] && return 1; log "step $1"; return 0; }
mark() { touch $here/state/$1; python3 $here/progress.py > $here/PROGRESS.md; log "done $1"; }
freeok() { awk -v f="$(df -B1 --output=avail / | tail -1)" 'BEGIN{exit !(f >= 2.7e9)}' || { log "free disk < 2.7 GB (SeaweedFS stops writing below 2.56 GB; hard floor 2.5), stop"; exit 1; }; }
sh $S/start-services.sh
for i in $(seq 1 120); do curl -s -m 3 -o /dev/null http://127.0.0.1:18123/ping && curl -sf -m 3 -o /dev/null 'http://127.0.0.1:18888/buckets/otel/?limit=1' -H 'Accept: application/json' && break; sleep 2; done
bash $here/swvac.sh off; bash $here/swvac.sh now
bash $here/chpriv.sh start   # query_log for central and reads-ch
freeok
if step bisect; then bash $here/bisect/run.sh >> $here/bisect/run.log 2>&1 && [ $(wc -l < $here/bisect/done.txt) -ge 15 ] && mark bisect; fi
[ -f $here/state/bisect ] || exit 1
if step build; then
  (cd $spike/otap-rs && cargo build --release --bin encbench --bin consume --bin otap-s3pq 2>&1 | tail -3) || exit 1
  mkdir -p $S/sorting/bin/sort
  cp -f $CARGO_TARGET_DIR/release/{encbench,consume,otap-s3pq} $S/sorting/bin/sort/ && mark build
fi
if step tests; then
  # the existing suites (lib, series, metrics, determinism, mbt) plus the new sort tests
  (cd $spike/otap-rs && export PATH=/opt/node22/bin:$PATH && set -o pipefail && {
    cargo test --release --lib 2>&1 | tail -4
    OTAPRS_DATA=$S/otaprs/data cargo test --release --test determinism --test otap_view -- --nocapture 2>&1 | grep -E "sort k=|test result|FAILED|panicked" 
    OTAPRS_DATA=$S/otaprs-mdata cargo test --release --test metrics 2>&1 | tail -3
    OTAPRS_DATA=$S/otaprs-mdata OTAPRS_SERIES_GO=$S/series/go OTAPRS_SERIES_FLEET=$S/series/fleet cargo test --release --test series 2>&1 | tail -3
    QUINT_SEED=0x5eed cargo test --release --test mbt_s3inline 2>&1 | tail -3
    QUINT_SEED=0x5eed cargo test --release --test mbt_s3inline_metrics 2>&1 | tail -3
    QUINT_SEED=0x5eed cargo test --release --test mbt_s3inline_consumer 2>&1 | tail -3
    cargo test --release --bin consume 2>&1 | tail -3
  }) > $here/tests.log 2>&1
  grep -q "FAILED\|panicked\|error\[" $here/tests.log && log "TESTS FAILED, see tests.log" || mark tests
fi
if step data; then bash $here/prep.sh data && python3 $here/reads.py services && mark data; fi
freeok
if step edge-cpu; then PASS=cpu bash $here/edge.sh >> $here/edge.log 2>&1
  [ $(grep -c '"pass": "cpu"' $here/edge.jsonl) -ge 80 ] && mark edge-cpu; fi
[ -f $here/state/edge-cpu ] || exit 1
if step edge-set; then PASS=set bash $here/edge.sh >> $here/edge.log 2>&1
  [ $(grep -c '"pass": "set"' $here/edge.jsonl) -ge 16 ] && mark edge-set; fi
if step central; then bash $here/central.sh >> $here/central.log 2>&1
  [ $(cut -d, -f1 $here/raw/tables.jsonl | sort -u | wc -l) -ge 40 ] && mark central; fi
[ -f $here/state/central ] || exit 1
if step reads-direct; then python3 $here/reads.py direct set && mark reads-direct; fi
if step reads-ch; then python3 $here/reads.py ch a-unsorted,b-sorted-1rg,c-hash-4rg,d-hash-16rg,e-range-4rg,f-range-16rg,g-hash-4rg-bloom,h-hash-16rg-bloom \
  && python3 $here/reads.py chlog && mark reads-ch; fi
freeok
if step route; then bash $here/prep.sh route >> $here/route.log 2>&1 && mark route; fi
if step reads-route; then
  python3 $here/reads.py direct $(python3 -c "
import json
print(' '.join('route/' + json.loads(l)['tag'] for l in open('$here/data/route-done.jsonl')))") && mark reads-route
fi
log "all steps done"
