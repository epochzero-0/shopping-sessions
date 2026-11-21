# infinity: Shopping Sessions

Team infinity's entry for the Builders Buildathon Databricks Shopping Sessions track. `RULES.md` is the spec.

- `src/main.cpp`: the pipeline. One static C++17 binary, no dependencies: memory-mapped CSVs parsed on all cores,
  hash-partitioned dedup, union-find over linked IDs, per-person sessionizing and click credit, and a minimal
  hand-written Parquet writer. The `Dockerfile` builds it in `gcc:14` and runs it from `scratch`.
- `ref/reference.py`: plain-Python correctness oracle (needs `pyarrow`).
- `fixtures/`, `scripts/`: the track's 36 edge cases and test harness, copied unchanged from the DuckDB starter.

```sh
python scripts/run_fixtures.py fixtures -- python ref/reference.py
```

## Third-party code

- `fixtures/`, `scripts/`, `RULES.md`: from the track's starter repository.
