#!/bin/sh
# A/B two images on 20M: alternate A and B, N runs each (default 7), BENCH=3 per container, report medians.
# ab.sh IMAGE_A IMAGE_B [N]      e.g.  docker tag shopping-sessions:latest shopping-sessions:base  (before rebuilding)
#                                      sh scripts/ab.sh shopping-sessions:base shopping-sessions:latest
# Raw lines: build/ab.txt. Output goes to the bind mount, so totals include ~0.5 s of write.
set -u
cd "$(dirname "$0")/.."
A=$1; B=$2; N=${3:-7}
mkdir -p build; O=build/ab.txt; : > "$O"
i=0
while [ $i -lt "$N" ]; do
  for img in "$A" "$B"; do
    r=$(IMAGE=$img BENCH=3 sh scripts/docker_run_win.sh "vol:${VOL:-infv3}" out_big 2>&1)
    echo "$img $(echo "$r" | grep '^run 1:') | $(echo "$r" | grep '^BENCH')" >> "$O"
  done
  i=$((i + 1))
done
med() { sort -n | awk '{a[NR]=$1} END{printf "%s (%s-%s)", a[int((NR+1)/2)], a[1], a[NR]}'; }
for img in "$A" "$B"; do
  L=$(grep "^$img " "$O")
  printf '%s  total %s  run1 %s' "$img" "$(echo "$L" | sed 's/.*total \([0-9.]*\) s.*/\1/' | med)" \
    "$(echo "$L" | sed 's/.*run 1: \([0-9.]*\) s.*/\1/' | med)"
  for ph in links "parse events+clicks" dedup sessions write; do
    printf '  %s %s' "${ph%% *}" "$(echo "$L" | sed "s/.*$ph \([0-9.]*\) .*/\1/" | med)"
  done
  echo
done
