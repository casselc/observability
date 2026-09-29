#!/usr/bin/env bash
# Runs the user journeys against a fresh lake UI rig and writes their
# pictures: one PNG per step (<out>/<journey>/<nn>-<slug>.png, re-encoded as
# a palette PNG) and one GIF per journey (<out>/<journey>.gif). The pictures
# are replaced only if every journey passed; a failed assertion leaves <out>
# as it was and fails the script.
#
#   cd otel-chdb/lakeui && npm ci
#   QS_IT_BIN=<dir with otelcol-s3pq and consume> e2e/journeys/render.sh [out]
#
# out defaults to otel-chdb/docs/journeys/img (the committed pictures).
# Needs what `npm run e2e` needs (ClickHouse :18123, SeaweedFS :18333, Go
# for the rig unless LAKEUI_RIG_BIN names a built one) and ffmpeg; without
# ffmpeg (JOURNEYS_NO_FFMPEG=1) the PNGs are copied as taken and no GIF is made.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
lakeui=$(cd "$here/../.." && pwd)
out=${1:-$lakeui/../docs/journeys/img}
work=$(mktemp -d "${TMPDIR:-/tmp}/journeys.XXXXXX")
trap 'rm -rf "$work"' EXIT

have_ffmpeg=1
if ! command -v ffmpeg >/dev/null; then
  if [ "${JOURNEYS_NO_FFMPEG:-}" = 1 ]; then
    have_ffmpeg=0
    echo "render.sh: no ffmpeg: PNGs unoptimised, no GIFs" >&2
  else
    echo "render.sh: ffmpeg not found (JOURNEYS_NO_FFMPEG=1 to go without)" >&2
    exit 1
  fi
fi

cd "$lakeui"
JOURNEYS_OUT="$work/img" JOURNEYS_FRAMES="$work/frames" \
  npx playwright test -c e2e/journeys/playwright.config.mjs

# The GIF and the PNGs of a journey (the frames: full viewport, 1200 x 820).
fps=0.4    # 2.5 s per step
gif_w=900
for dir in "$work"/img/*/; do
  name=$(basename "$dir")
  if [ "$have_ffmpeg" = 1 ]; then
    for f in "$dir"*.png; do
      # UI screenshots have few colours: a per-picture palette, no dither
      ffmpeg -loglevel error -y -i "$f" \
        -vf "split[a][b];[a]palettegen=max_colors=256:stats_mode=single:reserve_transparent=0[p];[b][p]paletteuse=dither=none" \
        -pix_fmt pal8 "$f.pal.png"
      if [ "$(stat -c %s "$f.pal.png")" -lt "$(stat -c %s "$f")" ]; then mv -f "$f.pal.png" "$f"; else rm -f "$f.pal.png"; fi
    done
    # hold the last step twice as long: repeat its frame
    frames="$work/frames/$name"
    last=$(ls "$frames"/*.png | tail -1)
    n=$(ls "$frames"/*.png | wc -l)
    cp -f "$last" "$frames/$(printf %02d $((n + 1))).png"
    ffmpeg -loglevel error -y -framerate "$fps" -i "$frames/%02d.png" \
      -vf "fps=$fps,scale=$gif_w:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=128:stats_mode=full[p];[b][p]paletteuse=dither=none:diff_mode=rectangle" \
      -loop 0 "$work/img/$name.gif"
  fi
done

# Only now replace the pictures: every journey passed.
mkdir -p "$out"
for dir in "$work"/img/*/; do
  name=$(basename "$dir")
  rm -rf "${out:?}/$name" "$out/$name.gif"
  cp -rf "$dir" "$out/$name"
  [ -f "$work/img/$name.gif" ] && cp -f "$work/img/$name.gif" "$out/$name.gif"
done
echo "render.sh: pictures in $out"
du -ab "$out" | sort -k2 | awk '{printf "%9d  %s\n", $1, $2}'
