#!/bin/sh
# runc wrapper for kind on a host whose container sandbox lacks
# CAP_SYS_RESOURCE (the dev box: `capsh --print` shows !cap_sys_resource).
# There, kubelet's negative oom_score_adj for critical and Guaranteed pods
# (-998, -997) cannot be applied, runc's nsexec fails with "failed to update
# /proc/self/oom_score_adj: Permission denied", and no pod sandbox starts
# ("can't get final child's PID from pipe: EOF"). This clamps the bundle's
# process.oomScoreAdj to our own value before handing over to runc.
# Wired in by kind-config.yaml's containerdConfigPatches (BinaryName) and
# mounted into the node with extraMounts; not needed on a normal host.
min=$(cat /proc/self/oom_score_adj)
prev=
for a in "$@"; do
  if [ "$prev" = "--bundle" ] || [ "$prev" = "-b" ]; then
    f="$a/config.json"
    v=$(jq '.process.oomScoreAdj // empty' "$f" 2>/dev/null)
    if [ -n "$v" ] && [ "$v" -lt "$min" ]; then
      jq ".process.oomScoreAdj = $min" "$f" > "$f.clamp" && mv -f "$f.clamp" "$f"
    fi
  fi
  prev=$a
done
exec /usr/local/sbin/runc "$@"
