#!/usr/bin/env bash
# Installs quint $QUINT_VERSION from npm and the Rust evaluator that version
# expects, into $QUINT_HOME (default ~/.quint), unless it is already there.
#
#   QUINT_VERSION=0.32.0 ci/install-quint.sh
#
# quint fetches its evaluator itself on first use, but through the GitHub
# REST API, unauthenticated: 60 requests an hour per IP, which the hosted
# runners share, so the fetch fails with "Failed to fetch from GitHub:
# Forbidden" often enough to fail a job. The release asset's plain download
# URL is not rate limited that way.
set -euo pipefail

: "${QUINT_VERSION:?}"
npm install -g "@informalsystems/quint@${QUINT_VERSION}"
quint --version

bm="$(npm root -g)/@informalsystems/quint/dist/src/rust/binaryManager.js"
ev="$(node -p "require('$bm').QUINT_EVALUATOR_VERSION")"
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) asset=quint_evaluator-x86_64-unknown-linux-gnu.tar.gz ;;
  Linux-aarch64) asset=quint_evaluator-aarch64-unknown-linux-gnu.tar.gz ;;
  Darwin-arm64) asset=quint_evaluator-aarch64-apple-darwin.tar.gz ;;
  Darwin-x86_64) asset=quint_evaluator-x86_64-apple-darwin.tar.gz ;;
  *) echo "install-quint: no evaluator asset for $(uname -sm)" >&2; exit 1 ;;
esac

dir="${QUINT_HOME:-$HOME/.quint}/rust-evaluator-${ev}"
if [ -x "$dir/quint_evaluator" ]; then
  echo "quint evaluator ${ev}: $dir/quint_evaluator (present)"
  exit 0
fi
mkdir -p "$dir"
url="https://github.com/informalsystems/quint/releases/download/evaluator/${ev}/${asset}"
for delay in 2 4 8 16 0; do
  if curl -fsSL --retry 3 "$url" | tar -xz -C "$dir" quint_evaluator; then break; fi
  [ "$delay" -eq 0 ] && { echo "install-quint: could not fetch $url" >&2; exit 1; }
  sleep "$delay"
done
chmod +x "$dir/quint_evaluator"
echo "quint evaluator ${ev}: $dir/quint_evaluator (from $url)"
