#!/bin/sh
# Build, then fixtures + dev + seed 2 (5M) + seed 3 (2M) against the oracles, then 20M timing.
# Prints one line:  PASS fx 36/36 dev s2 s3 | 20M 4.80 s (BENCH min of 3: parse 1.6 ...)
# check.sh -q skips the 20M timing.  Logs: build/check/*.log
set -u
cd "$(dirname "$0")/.."
W="C:\\Program Files\\Git\\usr\\bin\\bash.exe"
L=build/check
mkdir -p "$L"
fail() { echo "FAIL $1 (see $L/$2)"; exit 1; }

docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest . >"$L/build.log" 2>&1 \
  || fail build build.log

python scripts/run_fixtures.py fixtures -- "$W" scripts/docker_run_win.sh >"$L/fx.log" 2>&1
FX=$(tail -1 "$L/fx.log" | cut -d' ' -f1)
[ "$FX" = "36/36" ] || fail "fx $FX" fx.log

for s in dev:upstream/dev-answer s2:out_ref_s2 s3:out_ref_s3; do
  d=${s%%:*}; ref=${s#*:}
  sh scripts/docker_run_win.sh "$d" "out_$d" >"$L/$d.run.log" 2>&1 || fail "$d run" "$d.run.log"
  python scripts/compare.py "out_$d" "$ref" >"$L/$d.log" 2>&1 || fail "$d DIFFER" "$d.log"
done

LINE="PASS fx $FX dev s2 s3"
if [ "${1:-}" != "-q" ]; then
  BENCH=${BENCH:-3} sh scripts/docker_run_win.sh vol:infbig out_big >"$L/20m.log" 2>&1 || fail "20M run" 20m.log
  B=$(grep '^BENCH' "$L/20m.log")
  TOT=$(echo "$B" | sed 's/.*total \([0-9.]*\) s.*/\1/')
  HWM=$(grep VmHWM "$L/20m.log" | awk '{printf "%.1f GB", $2/1048576}')
  LINE="$LINE | 20M $TOT s, peak $HWM ($(echo "$B" | sed 's/^BENCH //; s/ total.*//'))"
fi
echo "$LINE"
