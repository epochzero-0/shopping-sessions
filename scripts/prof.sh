#!/bin/sh
# prof.sh "SYMBOL SUBSTRING": perf record the 20M pipeline (image inf-prof, volume infbig), print symbols >= 0.8%, then
# annotate the first symbol containing the substring by source line (% of that symbol's samples).
cd "$(dirname "$0")/.."
MSYS_NO_PATHCONV=1 docker run --rm --privileged --cpus 8 --memory 12g -v "$(pwd -W)/src":/src:ro \
  -v infbig:/data:ro --tmpfs /o:size=1g -e NCPU=8 -e DATA_DIR=/data -e OUTPUT_DIR=/o -e PAT="$1" inf-prof sh -c '
  g++ -O3 -march=cascadelake -std=c++17 -g -fno-omit-frame-pointer -static -pthread -o /tmp/pl /src/*.cpp &&
  /tmp/pl 2>/dev/null; perf record -q -F 4000 -o /tmp/p.data /tmp/pl >/dev/null 2>&1;
  perf report -i /tmp/p.data --no-children --sort sym --percent-limit 0.8 --stdio 2>/dev/null | grep -v "^#" | grep -v "^$";
  SYM=$(perf report -i /tmp/p.data --no-children --sort sym --stdio 2>/dev/null | grep -F "$PAT" | head -1 | sed "s/.*\] //");
  echo "== $SYM";
  perf annotate -i /tmp/p.data --stdio -l --percent-type local-period "$SYM" 2>/dev/null | grep -E "\.(cpp|hpp):[0-9]+" | sort -rn | head -40'
