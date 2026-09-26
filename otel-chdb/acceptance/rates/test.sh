#!/bin/sh
# Tests the rates scripts against a fake Prometheus (testdata/fake_prom.py,
# fixtures.json) and a fake kubectl (testdata/fake-kubectl), and checks the
# calculator inputs they produce. Run from anywhere; prints PASS or FAIL.
set -eu
R=$(cd "$(dirname "$0")" && pwd)
W=$(mktemp -d "${TMPDIR:-/tmp}/rates-test.XXXXXX")
PORT=${PORT:-18990}
python3 "$R/testdata/fake_prom.py" "$PORT" &
FP=$!
trap 'kill $FP 2>/dev/null; rm -rf "$W"' EXIT INT TERM
i=0
until curl -s "http://127.0.0.1:$PORT/api/v1/query?query=up" >/dev/null 2>&1 || [ $i -ge 50 ]; do i=$((i + 1)); sleep 0.1; done

echo "== collect.py against the fake Prometheus (with --heavy and a --selector)"
python3 "$R/collect.py" --prom "http://127.0.0.1:$PORT" --heavy --out "$W/rates.json"
python3 "$R/collect.py" --prom "http://127.0.0.1:$PORT" --selector 'cluster="prod-1"' --no-peak --out "$W/rates-sel.json" 2>/dev/null
echo
echo "== to_calculator.py (avg)"
python3 "$R/to_calculator.py" "$W/rates.json"
echo
echo "== to_calculator.py (peak, JSON)"
python3 "$R/to_calculator.py" "$W/rates.json" --basis peak --json
echo
echo "== kubectl_sample.py with a fake kubectl, then to_calculator.py --sample"
KUBECTL="$R/testdata/fake-kubectl" python3 "$R/kubectl_sample.py" --pods 5 --nodes 2 --seed 1 --out "$W/sample.json" >/dev/null
python3 "$R/to_calculator.py" --sample "$W/sample.json"

fail=0
check() { # name got want
  if [ "$2" = "$3" ]; then echo "  ok   $1 = $2"; else echo "  FAIL $1: got $2, want $3"; fail=1; fi
}
echo
echo "== assertions"
J=$(python3 "$R/to_calculator.py" "$W/rates.json" --basis now --json)
g() { printf %s "$1" | python3 -c "import json,sys; print(json.load(sys.stdin).get('$2'))"; }
check "now: pods" "$(g "$J" pods)" 3000
check "now: nodes" "$(g "$J" nodes)" 200
check "now: spansPod (tempo 30000/3000)" "$(g "$J" spansPod)" 10.0
check "now: logsPod (9000/3000)" "$(g "$J" logsPod)" 3.0
check "now: logsNode (1000/200)" "$(g "$J" logsNode)" 5.0
check "now: seriesPod ((2.4M-1.29M)+0.6M)/3000" "$(g "$J" seriesPod)" 570
check "now: seriesNode (690k/200)" "$(g "$J" seriesNode)" 3450
check "now: interval" "$(g "$J" interval)" 30
J=$(python3 "$R/to_calculator.py" "$W/rates.json" --basis avg --json)
check "avg: spansPod (rate over [1d] ×0.9)" "$(g "$J" spansPod)" 9.0
J=$(python3 "$R/to_calculator.py" "$W/rates.json" --basis peak --json)
check "peak: spansPod (×1.6)" "$(g "$J" spansPod)" 16.0
J=$(python3 "$R/to_calculator.py" "$W/rates.json" --spans-source otelcol --basis now --json)
check "otelcol spans (61000/3000)" "$(g "$J" spansPod)" 20.33
J=$(python3 "$R/to_calculator.py" --sample "$W/sample.json" --json)
check "sample: logsPod (600 lines / 600 s)" "$(g "$J" logsPod)" 1.0
check "sample: seriesPod (cAdvisor 200 + 300×3/12)" "$(g "$J" seriesPod)" 275
check "sample: seriesNode (kubelet)" "$(g "$J" seriesNode)" 1500
S=$(python3 -c "import json; b=json.load(open('$W/rates-sel.json'))['blocks']; print(b['pods']['query'])")
check "selector injected" "$S" 'count(kube_pod_status_phase{phase="Running",cluster="prod-1"} == 1)'
S=$(python3 -c "import json; b=json.load(open('$W/rates-sel.json'))['blocks']; print(b['head_series']['query'])")
check "selector in empty braces" "$S" 'sum(prometheus_tsdb_head_series{cluster="prod-1"})'
[ $fail = 0 ] && echo PASS || { echo FAIL; exit 1; }
