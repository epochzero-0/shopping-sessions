#!/bin/sh
# Quote/field/timestamp unit tests: cases from ref/reference.py, csv.hpp compiled with ASan+UBSan in gcc:14.
# Prints one line: unit N/N.  Exit 1 on any mismatch or sanitizer report.
set -u
cd "$(dirname "$0")/.."
mkdir -p build
python tests/csv_cases.py build/csv_cases.txt || exit 1
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W)":/w:ro gcc:14-bookworm sh -c '
  g++ -O1 -g -std=c++17 -march=cascadelake -fsanitize=address,undefined -fno-sanitize-recover=all \
    -o /tmp/t /w/tests/csv_test.cpp && /tmp/t /w/build/csv_cases.txt'
