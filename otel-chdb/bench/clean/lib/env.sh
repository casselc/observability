# Sourced by every block script. Environment evidence for each run:
#   snap EVENT   one JSON line: time, event, load averages, run queue, and the
#                aggregate /proc/stat cpu jiffies (user nice system idle iowait
#                irq softirq steal), appended to $ENVLOG.
#   header       lscpu model, nproc, kernel, into $ENVLOG.
#   gate         wait until the 1-min load average is <= 0.45 (so a run
#                never starts above 0.5), up to GATE_MAX s (default 900).
# lib/envlog.py pairs start/end snapshots and computes steal% per run.
: "${ENVLOG:?set ENVLOG}"
snap() {
  local u n s i w q sq st l1 l5 l15 pr
  read -r _ u n s i w q sq st _ < /proc/stat
  read -r l1 l5 l15 pr _ < /proc/loadavg
  printf '{"t":%s,"ev":"%s","load1":%s,"load5":%s,"procs":"%s","cpu":[%s,%s,%s,%s,%s,%s,%s,%s]}\n' \
    "$(date +%s.%N)" "$(echo "$1" | tr -d '"\\' | cut -c1-160)" "$l1" "$l5" "$pr" "$u" "$n" "$s" "$i" "$w" "$q" "$sq" "$st" >> "$ENVLOG"
}
header() {
  printf '{"t":%s,"ev":"header %s","model":"%s","nproc":%s,"kernel":"%s","free_gb":%s}\n' "$(date +%s.%N)" "$1" \
    "$(lscpu | sed -n 's/^Model name: *//p')" "$(nproc)" "$(uname -r)" \
    "$(df -B1 --output=avail / | tail -1 | awk '{printf "%.2f", $1/1e9}')" >> "$ENVLOG"
}
gate() {
  local max=${GATE_MAX:-900} t=0
  while :; do
    local l1; read -r l1 _ < /proc/loadavg
    if awk -v l="$l1" 'BEGIN{exit !(l <= 0.45)}'; then break; fi
    [ $t -ge $max ] && { echo "gate: load still $l1 after ${max}s" >&2; snap "gate-timeout"; return 1; }
    sleep 5; t=$((t + 5))
  done
  snap "gate-ok waited=${t}s"
}
freegb() { df -B1 --output=avail / | tail -1 | awk '{printf "%.2f", $1/1e9}'; }
