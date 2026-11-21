#!/bin/sh
# Container wall-clock A/B on 20M (BENCH=1, bind-mount output): sees exit/unmap and the output flush, which ab.sh's
# in-process run 1 does not. Alternates A and B N times (default 7), prints medians and ranges. Raw: build/abwall.txt
# abwall.sh IMAGE_A IMAGE_B [N]
set -u
cd "$(dirname "$0")/.."
A=$1; B=$2; N=${3:-7}
mkdir -p build; O=build/abwall.txt; : > "$O"
i=0
while [ $i -lt "$N" ]; do
  for img in "$A" "$B"; do
    t0=$(date +%s.%N)
    IMAGE=$img BENCH=1 sh scripts/docker_run_win.sh vol:infbig out_big >/dev/null 2>&1
    t1=$(date +%s.%N)
    echo "$img $(awk -v a="$t0" -v b="$t1" 'BEGIN{print b - a}')" >> "$O"
  done
  i=$((i + 1))
done
for img in "$A" "$B"; do
  grep "^$img " "$O" | awk '{print $2}' | sort -n |
    awk -v n="$img" '{a[NR]=$1} END{printf "%s wall %.3f (%.3f-%.3f)\n", n, a[int((NR+1)/2)], a[1], a[NR]}'
done
