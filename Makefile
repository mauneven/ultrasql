# UltraSQL benchmark targets.
#
# bench-smoke   — what pre-push runs: one iteration per benchmark, no
#                 competitor-floor checks, ≤ 5 s total.
# bench-full    — full sweep (8 measured iterations, 2 warmup).  Run this
#                 locally before promoting a performance-sensitive commit.
# bench-record  — full sweep + write results to benchmarks/baselines/<stage>.json.
# tpch-validate — TPC-H result correctness check; use TPCH_QUERIES=... for one
#                 selected-query phase without reloading SF1 per query.
#
# See BENCHMARKS.md for methodology and the Results Directory Policy.

CARGO        ?= cargo
STAGE        ?= current
BENCH_JOBS   ?= $(shell sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
# Transient benchmark data dirs and cached inputs live OUTSIDE the repo so runs
# never bloat the working tree or target/. See benchmarks/scratch.sh.
ULTRASQL_BENCH_SCRATCH ?= $(shell echo "$${TMPDIR:-/tmp}")/ultrasql-bench
TPCH_DATA_DIR    ?= $(ULTRASQL_BENCH_SCRATCH)/tpch-scale1-real
TPCH_DUCKDB      ?= duckdb
TPCH_QUERIES     ?= all
TPCH_POOL_FRAMES ?= 262144

.PHONY: bench-smoke bench-full bench-record tpch-validate clean-scratch help

help:
	@echo "UltraSQL benchmark targets:"
	@echo "  make bench-smoke    fast smoke check (pre-push equivalent)"
	@echo "  make bench-full     full sweep — run before promoting a perf commit"
	@echo "  make bench-record   full sweep + write baselines/<stage>.json"
	@echo "  make tpch-validate  TPC-H correctness; override TPCH_QUERIES=4,11,16"
	@echo "  make clean-scratch  reclaim build/benchmark scratch disk (safe)"

bench-smoke:
	CARGO_INCREMENTAL=0 \
	$(CARGO) run --release \
	    --jobs $(BENCH_JOBS) \
	    --package ultrasql-bench \
	    --bin regression-gate \
	    -- \
	    --stage $(STAGE) \
	    --smoke

bench-full:
	CARGO_INCREMENTAL=0 \
	$(CARGO) run --release \
	    --jobs $(BENCH_JOBS) \
	    --package ultrasql-bench \
	    --bin regression-gate \
	    -- \
	    --stage $(STAGE) \
	    --iterations 8 \
	    --warmup 2

bench-record:
	CARGO_INCREMENTAL=0 \
	$(CARGO) run --release \
	    --jobs $(BENCH_JOBS) \
	    --package ultrasql-bench \
	    --bin regression-gate \
	    -- \
	    --stage $(STAGE) \
	    --iterations 8 \
	    --warmup 2 \
	    --update-baseline

tpch-validate:
	CARGO_INCREMENTAL=0 \
	ULTRASQL_TPCH_POOL_FRAMES=$(TPCH_POOL_FRAMES) \
	ULTRASQL_TPCH_PROGRESS=1 \
	$(CARGO) run --release \
	    --package ultrasql-bench \
	    --features sql-bench \
	    --bin tpch \
	    -- \
	    validate-results \
	    --keep-going \
	    --queries $(TPCH_QUERIES) \
	    --data-dir $(TPCH_DATA_DIR) \
	    --duckdb $(TPCH_DUCKDB)

clean-scratch:
	ULTRASQL_BENCH_SCRATCH="$(ULTRASQL_BENCH_SCRATCH)" scripts/clean-scratch.sh
