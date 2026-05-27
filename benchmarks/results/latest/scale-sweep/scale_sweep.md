## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; competitors use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | SQLite | PostgreSQL | ClickHouse | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **6.79 ms** | 66.23 ms | 19.27 ms | 50.50 ms | - | UltraSQL |
| INSERT throughput | 100 000 | **59.75 ms** | 409.01 ms | 62.37 ms | 193.88 ms | - | UltraSQL |
| INSERT throughput | 1 000 000 | **639.64 ms** | 3929.79 ms | 642.38 ms | 2108.27 ms | - | UltraSQL |
| SELECT scan | 10 000 | **685.38 µs** | 953.21 µs | 1.95 ms | 30.66 ms | - | UltraSQL |
| SELECT scan | 100 000 | **6.87 ms** | 9.20 ms | 19.78 ms | 59.29 ms | - | UltraSQL |
| SELECT scan | 1 000 000 | **67.71 ms** | 95.34 ms | 203.26 ms | 210.67 ms | - | UltraSQL |
| SELECT SUM(x) | 10 000 | **70.62 µs** | 93.31 µs | 136.21 µs | 25.61 ms | - | UltraSQL |
| SELECT SUM(x) | 100 000 | **74.75 µs** | 104.44 µs | 1.44 ms | 36.69 ms | - | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **63.37 µs** | 174.21 µs | 13.84 ms | 43.73 ms | - | UltraSQL |
| SELECT AVG(x) | 10 000 | **76.67 µs** | 94.19 µs | 149.25 µs | 25.35 ms | - | UltraSQL |
| SELECT AVG(x) | 100 000 | **74.75 µs** | 131.54 µs | 1.48 ms | 38.98 ms | - | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **64.62 µs** | 242.44 µs | 14.54 ms | 40.82 ms | - | UltraSQL |
| Filter + SUM | 10 000 | **70.33 µs** | 103.02 µs | 153.38 µs | 26.14 ms | - | UltraSQL |
| Filter + SUM | 100 000 | **73.38 µs** | 136.62 µs | 1.60 ms | 37.06 ms | - | UltraSQL |
| Filter + SUM | 1 000 000 | **63.87 µs** | 186.00 µs | 16.39 ms | 41.28 ms | - | UltraSQL |
| UPDATE throughput | 10 000 | **120.67 µs** | 171.35 µs | 407.62 µs | 44.33 ms | - | UltraSQL |
| UPDATE throughput | 100 000 | **429.88 µs** | 778.50 µs | 4.21 ms | 172.34 ms | - | UltraSQL |
| UPDATE throughput | 1 000 000 | **2.10 ms** | 2.15 ms | 42.39 ms | 1953.68 ms | - | UltraSQL |
| DELETE throughput | 10 000 | **167.33 µs** | 2.08 ms | 538.62 µs | 21.57 ms | - | UltraSQL |
| DELETE throughput | 100 000 | **724.58 µs** | 19.90 ms | 5.88 ms | 37.02 ms | - | UltraSQL |
| DELETE throughput | 1 000 000 | **6.29 ms** | 220.82 ms | 59.43 ms | 186.19 ms | - | UltraSQL |
| Mixed OLTP | 10 000 | **168.96 µs/op** | 1.26 ms/op | 354.82 µs/op | 11.30 ms/op | - | UltraSQL |
| Window row_number() | 65 536 | **4.69 ms** | 7.32 ms | 30.04 ms | 53.10 ms | - | UltraSQL |
