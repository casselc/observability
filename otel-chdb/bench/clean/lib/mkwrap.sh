# mkwrap DIR NAME REAL CPUS [exec]
# Writes DIR/NAME, a stand-in for REAL that the upstream bench scripts call as
# $B/NAME or $T/NAME. It pins REAL to CPUS with taskset, and (unless "exec")
# brackets it with env snapshots ("start NAME args" / "end NAME rc") in
# $ENVLOG. "exec" mode keeps the pid (bench scripts read /proc/<pid> of a
# background otap-s3pq), so it only snapshots the start.
mkwrap() {
  local dir=$1 name=$2 real=$3 cpus=$4 mode=${5:-}
  mkdir -p "$dir"
  if [ "$mode" = exec ]; then
    cat > "$dir/$name" <<W
#!/bin/bash
ENVLOG=$ENVLOG; . $(dirname "${BASH_SOURCE[0]}")/env.sh
snap "exec $name \$*"
exec taskset -c $cpus $real "\$@"
W
  else
    cat > "$dir/$name" <<W
#!/bin/bash
ENVLOG=$ENVLOG; . $(dirname "${BASH_SOURCE[0]}")/env.sh
snap "start $name \$*"
taskset -c $cpus $real "\$@"; rc=\$?
snap "end $name rc=\$rc"
exit \$rc
W
  fi
  chmod +x "$dir/$name"
}
