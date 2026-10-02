#!/usr/bin/env bash
# The device forwarder (otel-chdb/forwarder, DECISIONS.md D37, D40 as amended 2026-10-02: a
# streaming pass-through): .NET build and tests, end to end against the Go ingress, and the
# nightly stress run. Runs the same on a workstation with
# the .NET 10 SDK and Go.
#
#   ci/forwarder.sh test     build everything with -warnaserror; the property and in-process
#                            proxy tests on real loopback sockets (not E2E, not Stress)
#   ci/forwarder.sh e2e      the Go ingress harness (ingress/cmd/ingress-e2e: the real handler,
#                            a fake Entra, an in-memory store, an answers log) and a forwarder
#                            process (tests/Oscope.Forwarder.E2EHost); the E2E tests drive them
#                            over HTTP. On Linux the forwarder runs under strace and must
#                            create, write, rename or delete no file (R-E7: no disk)
#   ci/forwarder.sh stress   the same stack (no strace), then the Stress tests for
#                            STRESS_SECONDS (default 300): many tools, rotating faults, every
#                            ingress answer matched with what a tool got, the memory bound
#
# Env: OUT (default ${RUNNER_TEMP:-/tmp}/forwarder) for logs and TRX; OSCOPE_TRACE_OUT, when
# set, receives one trace record per tagged test (ci/trace/dotnet_trx.py joins the claims
# with the TRX outcomes). The exit status is the tests' (a run that tested nothing fails).
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
fwd=$root/otel-chdb/forwarder
out=${OUT:-${RUNNER_TEMP:-/tmp}/forwarder}
mkdir -p "$out/trx"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
# No diagnostics IPC socket in the forwarder processes (it is a file in $TMPDIR).
export DOTNET_EnableDiagnostics=0
exe=
case "${OSTYPE:-}" in msys* | cygwin* | win32*) exe=.exe ;; esac
py=$(command -v python3 || command -v python)
tests=$fwd/tests/Oscope.Forwarder.Tests/Oscope.Forwarder.Tests.csproj
host_dll=$fwd/tests/Oscope.Forwarder.E2EHost/bin/Release/net10.0/oscope-forwarder-e2e.dll

build() {
  for p in src/Oscope.Forwarder.App/Oscope.Forwarder.App.csproj tests/Oscope.Forwarder.E2EHost/Oscope.Forwarder.E2EHost.csproj \
           tests/Oscope.Forwarder.Tests/Oscope.Forwarder.Tests.csproj; do
    dotnet build "$fwd/$p" -c Release -warnaserror -nologo -v minimal
  done
}

# run_tests NAME FILTER: dotnet test with a TRX, then the trace records; returns the tests' status
run_tests() {
  local name=$1 filter=$2 rc=0
  rm -f "$out/trx/$name.trx"
  dotnet test "$tests" -c Release --no-build --nologo --filter "$filter" \
    --logger "trx;LogFileName=$name.trx" --logger "console;verbosity=normal" --results-directory "$out/trx" || rc=$?
  if [ -n "${OSCOPE_TRACE_OUT:-}" ]; then
    "$py" "$root/ci/trace/dotnet_trx.py" --trx "$out/trx/$name.trx" --claims "$OSCOPE_TRACE_OUT.dotnet-claims.jsonl" \
      --out "$OSCOPE_TRACE_OUT" || rc=1
  fi
  return $rc
}

pids=() # macOS runs bash 3.2: an empty array under set -u is "unbound", hence ${pids[@]+...}
stop_stack() {
  local p
  # The forwarders first, by name: under strace, $! is strace's pid, not the forwarder's.
  if [ -z "$exe" ]; then pkill -TERM -f oscope-forwarder-e2e.dll 2>/dev/null || true; fi
  for p in ${pids[@]+"${pids[@]}"}; do kill -TERM "$p" 2>/dev/null || true; done
  if [ -n "$exe" ]; then
    # Windows: no SIGTERM for native processes; end them, and do not wait on them
    taskkill //F //IM ingress-e2e.exe >/dev/null 2>&1 || true
    for p in ${pids[@]+"${pids[@]}"}; do kill -KILL "$p" 2>/dev/null || true; done
  else
    for p in ${pids[@]+"${pids[@]}"}; do wait "$p" 2>/dev/null || true; done
  fi
  pids=()
}
trap stop_stack EXIT

wait_for() { # wait_for FILE PATTERN SECONDS
  local i
  for ((i = 0; i < $3 * 10; i++)); do
    if grep -q "$2" "$1" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "timed out waiting for '$2' in $1" >&2
  cat "$1" >&2 || true
  return 1
}

# start_stack STRACE(0|1): the harness and the forwarder; exports the E2E environment
start_stack() {
  (cd "$root/otel-chdb/ingress" && go build -o "$out/ingress-e2e$exe" ./cmd/ingress-e2e)
  "$out/ingress-e2e$exe" -listen 127.0.0.1:0 -requests_per_minute 1000000 -decoded_bytes_per_minute 1e12 \
    >"$out/harness.out" 2>"$out/harness.log" &
  pids+=($!)
  wait_for "$out/harness.out" '^listening ' 30
  local addr
  addr=$(sed -n 's/^listening //p' "$out/harness.out" | head -1 | tr -d '\r')
  export OSCOPE_E2E_INGRESS="http://$addr"
  # A small concurrency bound, so the stress run's 24 senders reach it; forced refreshes allowed
  # every second, so the E2E token test does not depend on the order the tests run in.
  local proxy='{"maxRequestBytes":16777216,"maxConcurrentRequests":8,"tokenTimeout":"00:00:05","activityTimeout":"00:00:30","refusalRetryAfter":"00:00:01","forcedRefreshMinInterval":"00:00:01"}'
  local -a run=(dotnet "$host_dll")
  if [ "$1" = 1 ]; then
    run=(strace -f -qq -o "$out/strace.txt" -e trace=open,openat,creat,mkdir,mkdirat,rename,renameat,renameat2,unlink,unlinkat,link,linkat,symlink,symlinkat,truncate "${run[@]}")
  fi
  OSCOPE_E2E_PORT=14318 OSCOPE_E2E_PROXY=$proxy "${run[@]}" >"$out/forwarder.out" 2>"$out/forwarder.log" &
  pids+=($!)
  export OSCOPE_E2E_FORWARDER_PID=$!
  wait_for "$out/forwarder.out" '^forwarder listening' 60
  export OSCOPE_E2E_FORWARDER=http://127.0.0.1:14318
}

# After the forwarder stopped: under strace, no file was written. (Nothing is held, so there
# is no ledger to balance at exit: what is not answered 200 by the ingress is the tool's.)
check_stack() {
  local rc=0
  if [ -z "$exe" ]; then cat "$out/forwarder.out" || true; fi
  if [ -f "$out/strace.txt" ]; then
    "$py" - "$out/strace.txt" <<'EOF' || rc=1
import re, sys
# A write-intent open, or any create / rename / delete / link / truncate of a path. Reads are fine.
allowed = ('/dev/', '/proc/', '/sys/')
bad = []
for line in open(sys.argv[1], encoding='utf-8', errors='replace'):
    m = re.search(r'\b(open|openat|creat|mkdir|mkdirat|rename|renameat2?|unlink|unlinkat|link|linkat|symlink|symlinkat|truncate)\((.*)', line)
    if not m or '= -1' in line:
        continue
    call, args = m.group(1), m.group(2)
    paths = re.findall(r'"([^"]*)"', args)
    if call in ('open', 'openat'):
        if not re.search(r'O_WRONLY|O_RDWR|O_CREAT|O_TRUNC|O_APPEND', args):
            continue
    if paths and all(p.startswith(allowed) for p in paths):
        continue
    bad.append(line.rstrip())
if bad:
    print('::error::the forwarder touched the file system (R-E7: nothing on disk):')
    print('\n'.join(bad[:50]))
    sys.exit(1)
print('no-disk check: no file created, written, renamed or deleted by the forwarder')
EOF
  fi
  return $rc
}

cmd=${1:?test|e2e|stress}
case "$cmd" in
  test)
    build
    run_tests unit 'Category!=E2E&Category!=Stress'
    ;;
  e2e)
    build
    use_strace=0
    if [ "$(uname -s)" = Linux ] && command -v strace >/dev/null; then use_strace=1; fi
    start_stack "$use_strace"
    rc=0
    run_tests e2e 'Category=E2E' || rc=$?
    stop_stack
    check_stack || rc=1
    exit $rc
    ;;
  stress)
    build
    start_stack 0
    rc=0
    OSCOPE_STRESS_SECONDS=${STRESS_SECONDS:-300} run_tests stress 'Category=Stress' || rc=$?
    stop_stack
    check_stack || rc=1
    exit $rc
    ;;
  *) echo "unknown command $cmd" >&2; exit 2 ;;
esac
