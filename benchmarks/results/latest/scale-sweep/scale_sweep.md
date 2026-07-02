## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.55 ms** | 32.75 ms (2015% slower) | 60.19 ms (3786.6% slower) | 1.68 ms (8.6% slower) | 3.07 ms (98.3% slower) | UltraSQL |
| INSERT throughput | 100 000 | **10.30 ms** | 319.68 ms (3003.3% slower) | 610.19 ms (5823.5% slower) | 17.28 ms (67.8% slower) | 20.68 ms (100.8% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **110.11 ms** | 3391.86 ms (2980.4% slower) | 6143.87 ms (5479.7% slower) | 240.89 ms (118.8% slower) | 253.81 ms (130.5% slower) | UltraSQL |
| SELECT scan | 10 000 | **692.56 µs** | 885.94 µs (27.9% slower) | 992.85 µs (43.4% slower) | 1.88 ms (170.8% slower) | 1.46 ms (110.6% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.23 ms** | 9.20 ms (47.8% slower) | 6.73 ms (8.1% slower) | 19.71 ms (216.6% slower) | 15.74 ms (152.8% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **59.92 ms** | 96.02 ms (60.2% slower) | 61.15 ms (2.1% slower) | 207.69 ms (246.6% slower) | 162.73 ms (171.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **41.35 µs** | 67.02 µs (62.1% slower) | 467.25 µs (1029.9% slower) | 136.79 µs (230.8% slower) | 282.65 µs (583.5% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **53.19 µs** | 87.31 µs (64.2% slower) | 655.77 µs (1132.9% slower) | 1.42 ms (2577.2% slower) | 2.36 ms (4343.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **127.02 µs** | 158.08 µs (24.5% slower) | 1.59 ms (1152.8% slower) | 16.12 ms (12594.8% slower) | 11.18 ms (8699.8% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **52.27 µs** | 70.15 µs (34.2% slower) | 442.71 µs (746.9% slower) | 137.04 µs (162.2% slower) | 313.06 µs (498.9% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **62.08 µs** | 113.81 µs (83.3% slower) | 682.67 µs (999.6% slower) | 1.41 ms (2179.1% slower) | 2.58 ms (4060.2% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **126.81 µs** | 224.10 µs (76.7% slower) | 1.58 ms (1143.8% slower) | 15.72 ms (12297.6% slower) | 11.84 ms (9235% slower) | UltraSQL |
| Filter + SUM | 10 000 | **42.69 µs** | 74.71 µs (75% slower) | 523.15 µs (1125.5% slower) | 152.69 µs (257.7% slower) | 312.04 µs (631% slower) | UltraSQL |
| Filter + SUM | 100 000 | **56.48 µs** | 121.25 µs (114.7% slower) | 764.21 µs (1253.1% slower) | 1.56 ms (2658.4% slower) | 2.56 ms (4430% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **137.02 µs** | 168.19 µs (22.7% slower) | 1.38 ms (905.3% slower) | 17.58 ms (12731.8% slower) | 11.78 ms (8500.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **120.83 µs** | 157.50 µs (30.3% slower) | 3.99 ms (3204.7% slower) | 483.17 µs (299.9% slower) | 4.02 ms (3229% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **369.54 µs** | 751.60 µs (103.4% slower) | 11.43 ms (2994.3% slower) | 5.50 ms (1388.2% slower) | 38.33 ms (10272.3% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.13 ms (18.9% slower) | **2.63 ms** | 33.68 ms (1179.8% slower) | 59.80 ms (2172.3% slower) | 1643.89 ms (62366.6% slower) | DuckDB |
| DELETE throughput | 10 000 | **97.10 µs** | 113.85 µs (17.3% slower) | 3.21 ms (3208.4% slower) | 592.19 µs (509.8% slower) | 1.36 ms (1297.2% slower) | UltraSQL |
| DELETE throughput | 100 000 | **369.71 µs** | 409.29 µs (10.7% slower) | 3.28 ms (788.1% slower) | 7.13 ms (1827.5% slower) | 12.28 ms (3222.6% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.64 ms (32.2% slower) | 4.41 ms (60% slower) | **2.75 ms** | 71.75 ms (2505.3% slower) | 355.62 ms (12813.4% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 130.19 µs/op (698.8% slower) | 145.94 µs/op (795.4% slower) | 26.62 ms/op (163252.6% slower) | **16.30 µs/op** | 34.20 µs/op (109.9% slower) | SQLite |
| Mixed correctness | 100 000 | **138.81 µs** | 266.29 µs (91.8% slower) | 74.05 ms (53243.3% slower) | 2.23 ms (1507.4% slower) | 3.20 ms (2207.4% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.88 ms** | 6.88 ms (41% slower) | 5.74 ms (17.6% slower) | 27.70 ms (467.4% slower) | 16.23 ms (232.4% slower) | UltraSQL |
