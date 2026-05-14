# UltraSQL benchmark targets.
#
# bench-smoke   — what pre-push runs: one iteration per benchmark, no
#                 competitor-floor checks, ≤ 5 s total.
# bench-full    — full sweep (8 measured iterations, 2 warmup).  Run this
#                 locally before promoting a performance-sensitive commit.
# bench-record  — full sweep + write results to benchmarks/baselines/<stage>.json.
#
# See BENCHMARKS.md for methodology and the Results Directory Policy.

CARGO        ?= cargo
STAGE        ?= current
BENCH_JOBS   ?= $(shell sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

.PHONY: bench-smoke bench-full bench-record help

help:
	@echo "UltraSQL benchmark targets:"
	@echo "  make bench-smoke    fast smoke check (pre-push equivalent)"
	@echo "  make bench-full     full sweep — run before promoting a perf commit"
	@echo "  make bench-record   full sweep + write baselines/<stage>.json"

bench-smoke:
	$(CARGO) run --release \
	    --jobs $(BENCH_JOBS) \
	    --package ultrasql-bench \
	    --bin regression-gate \
	    -- \
	    --stage $(STAGE) \
	    --smoke

bench-full:
	$(CARGO) run --release \
	    --jobs $(BENCH_JOBS) \
	    --package ultrasql-bench \
	    --bin regression-gate \
	    -- \
	    --stage $(STAGE) \
	    --iterations 8 \
	    --warmup 2

bench-record:
	$(CARGO) run --release \
	    --jobs $(BENCH_JOBS) \
	    --package ultrasql-bench \
	    --bin regression-gate \
	    -- \
	    --stage $(STAGE) \
	    --iterations 8 \
	    --warmup 2 \
	    --update-baseline
