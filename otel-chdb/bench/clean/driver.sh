#!/bin/bash
# driver.sh: blocks 2-5 in order, strictly sequentially. Restart-safe: the
# container is restarted about hourly and every process dies, so after a
# restart just run this again (setsid nohup bash driver.sh >> driver.log 2>&1 &).
# Each block script skips runs that already have a result, and this script
# starts exactly the services each block needs and checks they answer:
#   blocks 2-4  SeaweedFS pinned to CPU 0, the shared ClickHouse STOPPED, the
#               private one (lib/chpriv.sh, part_log + query_log) on CPUs 1-3
#   block 5     SeaweedFS on CPU 1, the shared ClickHouse on CPUs 2-3, the
#               private one stopped (consume is pinned to CPU 0 by run.sh)
# PROGRESS.md is regenerated from the files on disk after every step.
# ONLY=b2,b3 limits the blocks. Stops if free disk would fall below 2.5 GB.
set -u
here=$(cd "$(dirname "$0")" && pwd)
. $here/lib/services.sh
log() { echo "$(date -u +%FT%TZ) $*"; }
progress() { python3 $here/lib/progress.py > $here/PROGRESS.md.tmp && mv -f $here/PROGRESS.md.tmp $here/PROGRESS.md; }
diskok() {
  local f; f=$(df -B1 --output=avail / | tail -1 | awk '{printf "%.2f", $1/1e9}')
  if awk -v f="$f" 'BEGIN{exit !(f < 2.7)}'; then log "free disk $f GB < 2.7: stopping"; progress; exit 1; fi
}
want() { [ -z "${ONLY:-}" ] || [[ ",$ONLY," == *",$1,"* ]]; }
blocks_2_4_services() {
  weed_up 0 || { log "SeaweedFS not up"; exit 1; }
  shared_down || { log "shared ClickHouse would not stop"; exit 1; }
  priv_up 1-3 || { log "private ClickHouse not up"; exit 1; }
  log "services: weed (CPU 0), private CH $(curl -s http://127.0.0.1:18623/ --data-binary 'SELECT version()') (CPUs 1-3), shared CH down"
}
progress
log "driver start; uptime: $(uptime)"

if want b2 && [ "$(python3 $here/lib/progress.py --state b2)" != done ]; then
  blocks_2_4_services; diskok
  log "block 2"
  bash $here/block2/run.sh >> $here/block2/run.log 2>&1
  progress
  [ "$(python3 $here/lib/progress.py --state b2)" = done ] || { log "block 2 incomplete"; exit 1; }
fi
if want b2 && [ ! -s $here/block2/results.md ]; then
  python3 $here/block2/analyze.py --json > $here/block2/results.md; progress
fi

if want b3 && [ ! -f $here/block3/pool.ok ]; then
  weed_up 0; diskok
  log "block 3 prep (replay pool)"
  $S/clean/bin/mgen -clean clean/merges/pool/ > /dev/null 2>&1
  curl -s -X DELETE "http://127.0.0.1:18888/buckets/otel/clean/merges?recursive=true&ignoreRecursiveError=true" > /dev/null
  bash $here/block3/prep.sh > $here/block3/prep.log 2>&1 && touch $here/block3/pool.ok || { log "block 3 prep failed"; exit 1; }
  progress
fi

block4() { # [B4_LONG=1]: also the no-replay jobs on the long pool
  blocks_2_4_services; diskok
  log "block 4 ${B4_LONG:+(no-replay jobs)}"
  ENVLOG=$here/block4/env.jsonl bash -c ". $here/lib/env.sh; header 'block4 before'; gate; snap 'begin b4${B4_LONG:+-long}'"
  B4_LONG=${B4_LONG:-} taskset -c 0 python3 $here/block4/stored.py >> $here/block4/run.log 2>&1
  ENVLOG=$here/block4/env.jsonl bash -c ". $here/lib/env.sh; snap 'finish b4${B4_LONG:+-long}'; header 'block4 after'"
  progress
}
# block 4, first pass (the replayed pool); the no-replay pass runs after block 3, when there is disk for the long pool
if want b4 && ! grep -q '"label": "A-summary"' $here/block4/results.jsonl 2>/dev/null; then
  block4
  grep -q '"label": "A-summary"' $here/block4/results.jsonl || { log "block 4 incomplete"; exit 1; }
fi

if want b3 && [ "$(python3 $here/lib/progress.py --state b3)" != done ]; then
  blocks_2_4_services; diskok
  log "block 3"
  bash $here/block3/run.sh
  progress
  [ "$(python3 $here/lib/progress.py --state b3)" = done ] || { log "block 3 incomplete"; exit 1; }
fi

if want b4 && [ "$(python3 $here/lib/progress.py --state b4)" != done ]; then
  if [ ! -f $here/block4/long.ok ]; then
    weed_up 0; diskok
    log "block 4: long pool (mgen, 120 rounds, no replay)"
    $S/clean/bin/mgen -clean clean/merges/pool/long/ > /dev/null 2>&1
    (cd $S/clean/bin && ./mgen -prefix clean/merges/pool/long -rounds 120 -pods-per-batch 200 -a) >> $here/block4/prep-long.log 2>&1 \
      && touch $here/block4/long.ok || { log "long pool failed"; exit 1; }
  fi
  B4_LONG=1 block4
  [ "$(python3 $here/lib/progress.py --state b4)" = done ] || { log "block 4 (no replay) incomplete"; exit 1; }
fi

if want b5 && [ "$(python3 $here/lib/progress.py --state b5)" != done ]; then
  priv_down
  weed_up 1 || { log "SeaweedFS not up"; exit 1; }
  shared_up 2-3 || { log "shared ClickHouse not up"; exit 1; }
  diskok
  log "block 5: weed on CPU 1, shared CH on CPUs 2-3"
  bash $here/block5/run.sh
  progress
fi
log "driver done"
