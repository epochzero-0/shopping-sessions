# infinity: Shopping Sessions

Team infinity's entry for the Builders Buildathon Databricks Shopping Sessions track. `RULES.md` is the spec.

- `ref/reference.py`: plain-Python correctness oracle (needs `pyarrow`).
- `fixtures/`, `scripts/`: the track's 36 edge cases and test harness, copied unchanged from the DuckDB starter.

```sh
python scripts/run_fixtures.py fixtures -- python ref/reference.py
```

## Third-party code

- `fixtures/`, `scripts/`, `RULES.md`: from the track's starter repository.
