#!/bin/sh
# docker_run.sh for Git Bash on Windows: mounts take C:/ paths and MSYS must not rewrite arguments.
# docker_run_win.sh DATA_DIR OUTPUT_DIR   (DATA_DIR may also be a docker volume name: vol:NAME)
set -eu
IMAGE="${IMAGE:-shopping-sessions:latest}"
CPUS="${CPUS:-8}"
MEM="${MEM:-12g}"
case "$1" in
  vol:*) DATA="${1#vol:}" ;;
  *) DATA=$(cd "$1" && pwd -W) ;;
esac
mkdir -p "$2"; OUT=$(cd "$2" && pwd -W)
rm -rf "$2"/user_daily "$2"/campaign_daily
SCRATCH=$(mktemp -d); SCRATCH_W=$(cd "$SCRATCH" && pwd -W)
trap 'rm -rf "$SCRATCH"' EXIT
MSYS_NO_PATHCONV=1 docker run --rm --platform linux/amd64 \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$DATA":/data:ro -v "$OUT":/output -v "$SCRATCH_W":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" -e BENCH="${BENCH:-1}" \
  "$IMAGE"
