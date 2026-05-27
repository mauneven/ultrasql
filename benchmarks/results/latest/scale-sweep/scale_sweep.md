## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; competitors use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **6.62 ms** | 62.64 ms | 18.75 ms | 50.57 ms | UltraSQL |
| INSERT throughput | 100 000 | 69.44 ms | 402.55 ms | **65.86 ms** | 179.95 ms | SQLite |
| INSERT throughput | 1 000 000 | - | 3814.02 ms | **790.31 ms** | 2932.01 ms | SQLite |
| SELECT scan | 10 000 | **714.54 µs** | 866.00 µs | 1.84 ms | 28.22 ms | UltraSQL |
| SELECT scan | 100 000 | **7.07 ms** | 9.90 ms | 19.18 ms | 56.00 ms | UltraSQL |
| SELECT scan | 1 000 000 | **68.79 ms** | 94.88 ms | 202.34 ms | 204.61 ms | UltraSQL |
| SELECT SUM(x) | 10 000 | 74.58 µs | **68.62 µs** | 138.31 µs | 24.18 ms | DuckDB |
| SELECT SUM(x) | 100 000 | **59.62 µs** | 104.58 µs | 1.42 ms | 33.31 ms | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **63.00 µs** | 164.08 µs | 14.04 ms | 40.51 ms | UltraSQL |
| Filter + SUM | 10 000 | **62.38 µs** | 108.48 µs | 155.83 µs | 26.39 ms | UltraSQL |
| Filter + SUM | 100 000 | **71.50 µs** | 141.19 µs | 1.57 ms | 36.04 ms | UltraSQL |
| Filter + SUM | 1 000 000 | **64.50 µs** | 180.56 µs | 15.80 ms | 40.15 ms | UltraSQL |
| UPDATE throughput | 10 000 | **109.75 µs** | 167.58 µs | 418.17 µs | 42.24 ms | UltraSQL |
| UPDATE throughput | 100 000 | **434.83 µs** | 773.10 µs | 4.16 ms | 159.75 ms | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.94 ms | **2.21 ms** | 45.39 ms | 1923.45 ms | DuckDB |
