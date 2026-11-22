#!/bin/sh
# Correctness net: 20 small generated datasets (seed:events) vs ref/reference.py and the Docker image.
# GEN=generator_v2 uses upstream/generator_v2 (IDs with 32-char hex suffixes); datasets cached per generator.
# Prints failing seed names, then one line:  seeds 20/20   (exit 1 if any fail)
# Datasets are cached in build/seeds/data/; oracle + image always rerun. Logs: build/seeds/*.log
set -u
cd "$(dirname "$0")/.."
B=build/seeds
mkdir -p "$B/data"
REPO=$(pwd -W)
CASES="11:1000 12:1000 13:1000 14:1000 15:1000 16:3000 17:5000 18:10000 19:10000 20:20000
21:30000 22:50000 23:50000 24:80000 25:100000 26:100000 27:150000 28:200000 29:200000 30:200000"

gen() { # gen SEED EVENTS DIR
  rm -rf "$3.tmp"; mkdir -p "$3.tmp"
  MSYS_NO_PATHCONV=1 docker run --rm -v "$REPO":/repo -w /repo rust:1.85 \
    "upstream/${GEN:-generator}/target/release/generate" --seed "$1" --events "$2" --clicks $(($2 / 4)) \
    --output "/repo/$3.tmp" >"$B/$4.gen.log" 2>&1 && { mv "$3.tmp" "$3" 2>/dev/null || { sleep 2; mv "$3.tmp" "$3"; }; }
}

FAILED=""; PASS=0; TOTAL=0
for c in $CASES; do
  s=${c%%:*}; n=${c#*:}; name="${GEN:+${GEN#generator}_}s${s}_${n}"
  TOTAL=$((TOTAL + 1))
  D="$B/data/$name"
  if [ ! -f "$D/manifest.json" ]; then
    gen "$s" "$n" "$D" "$name" || { FAILED="$FAILED $name(gen)"; continue; }
  fi
  rm -rf "$B/$name.ref" "$B/$name.out"
  python ref/reference.py "$D" "$B/$name.ref" >"$B/$name.ref.log" 2>&1 \
    || { FAILED="$FAILED $name(oracle)"; continue; }
  sh scripts/docker_run_win.sh "$D" "$B/$name.out" >"$B/$name.run.log" 2>&1 \
    || { FAILED="$FAILED $name(run)"; continue; }
  if python scripts/compare.py "$B/$name.out" "$B/$name.ref" >"$B/$name.cmp.log" 2>&1; then
    PASS=$((PASS + 1)); rm -rf "$B/$name.ref" "$B/$name.out"
  else
    FAILED="$FAILED $name"
  fi
done

for f in $FAILED; do echo "FAIL $f"; done
echo "seeds $PASS/$TOTAL"
[ "$PASS" = "$TOTAL" ]
