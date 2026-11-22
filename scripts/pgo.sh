#!/bin/sh
# pgo.sh: retrain pgo/*.gcda on 20M (volume $VOL, default infv3) with the Dockerfile's exact flags and paths, then check that a
# -fprofile-use build accepts the new profile with no missing or mismatched function. Rerun after every src/ change.
set -eu
cd "$(dirname "$0")/.."
rm -rf pgo && mkdir pgo
MSYS_NO_PATHCONV=1 docker run --rm --cpus 8 --memory 8g -v "$(pwd -W)/src":/in/src:ro -v "$(pwd -W)/pgo":/out \
  -v "${VOL:-infv3}":/data:ro --tmpfs /o:size=1g -e NCPU=8 -e DATA_DIR=/data -e OUTPUT_DIR=/o gcc:14-bookworm sh -ec '
  mkdir -p /src && cp -r /in/src /src/src && cd /src
  # The pipeline leaves through _exit, which skips the atexit counter dump: dump first in the training build only.
  printf "extern void __gcov_dump(void); extern void __real__exit(int);\nvoid __wrap__exit(int s) { __gcov_dump(); __real__exit(s); }\n" > /tmp/dump.c
  gcc -O2 -c /tmp/dump.c -o /tmp/dump.o
  F="-O3 -march=cascadelake -std=c++17 -flto -static -pthread -fprofile-prefix-path=/src"
  g++ $F -fprofile-generate=pgo -fprofile-update=prefer-atomic -Wl,--wrap=_exit -o pipeline src/*.cpp /tmp/dump.o
  ./pipeline > /dev/null
  g++ $F -fprofile-use=pgo -Werror=missing-profile -Werror=coverage-mismatch -o pipeline src/*.cpp
  cp pgo/*.gcda /out/ && ls -l /out'
