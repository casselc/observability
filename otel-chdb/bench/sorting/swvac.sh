#!/bin/bash
# SeaweedFS housekeeping, never inside a measured window.
#   swvac.sh off   stop the master's periodic vacuum (every 15 min, it would
#                  land inside measured runs); lost when weed restarts, so
#                  driver.sh runs it after start-services.sh
#   swvac.sh now   compact the otel volumes: deleted objects keep their disk
#                  space until a vacuum, and SeaweedFS stops accepting writes
#                  below 1% free disk (2.56 GB here)
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
case ${1:-now} in
off) printf 'lock\nvolume.vacuum.disable\nunlock\nexit\n' ;;
now) printf 'lock\nvolume.vacuum -garbageThreshold 0.02 -collection otel\nunlock\nexit\n' ;;
esac | timeout 900 $S/gobin/weed shell -master=127.0.0.1:19333 > /dev/null 2>&1
