## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.48 ms** | 31.31 ms (2022.6% slower) | 62.51 ms (4137.2% slower) | 1.61 ms (8.9% slower) | 2.96 ms (100.7% slower) | UltraSQL |
| INSERT throughput | 100 000 | **9.81 ms** | 326.33 ms (3224.9% slower) | 634.90 ms (6368.9% slower) | 17.42 ms (77.4% slower) | 23.37 ms (138.1% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **109.25 ms** | 3216.22 ms (2844% slower) | 6510.90 ms (5859.7% slower) | 225.67 ms (106.6% slower) | 247.64 ms (126.7% slower) | UltraSQL |
| SELECT scan | 10 000 | **728.54 µs** | 937.31 µs (28.7% slower) | 962.33 µs (32.1% slower) | 1.91 ms (161.9% slower) | 1.39 ms (90.9% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.54 ms** | 9.34 ms (42.9% slower) | 7.67 ms (17.4% slower) | 20.26 ms (210% slower) | 16.48 ms (152% slower) | UltraSQL |
| SELECT scan | 1 000 000 | 61.48 ms (5.1% slower) | 92.16 ms (57.5% slower) | **58.50 ms** | 202.91 ms (246.9% slower) | 158.81 ms (171.5% slower) | ClickHouse |
| SELECT SUM(x) | 10 000 | **51.10 µs** | 70.21 µs (37.4% slower) | 428.02 µs (737.5% slower) | 138.79 µs (171.6% slower) | 294.88 µs (477% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **47.79 µs** | 85.00 µs (77.9% slower) | 643.50 µs (1246.5% slower) | 1.42 ms (2866.7% slower) | 2.36 ms (4847.1% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **122.94 µs** | 156.85 µs (27.6% slower) | 1.57 ms (1174.7% slower) | 15.59 ms (12581.6% slower) | 10.95 ms (8805.8% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **43.71 µs** | 69.44 µs (58.9% slower) | 446.92 µs (922.5% slower) | 136.40 µs (212.1% slower) | 317.35 µs (626.1% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **50.46 µs** | 115.13 µs (128.2% slower) | 679.44 µs (1246.5% slower) | 1.45 ms (2776.9% slower) | 2.62 ms (5087.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **123.15 µs** | 238.63 µs (93.8% slower) | 1.66 ms (1247.7% slower) | 15.59 ms (12562.3% slower) | 11.44 ms (9189.8% slower) | UltraSQL |
| Filter + SUM | 10 000 | **42.56 µs** | 76.73 µs (80.3% slower) | 535.79 µs (1158.8% slower) | 152.60 µs (258.5% slower) | 308.54 µs (624.9% slower) | UltraSQL |
| Filter + SUM | 100 000 | **61.67 µs** | 126.60 µs (105.3% slower) | 746.19 µs (1110% slower) | 1.58 ms (2462.2% slower) | 2.57 ms (4070.2% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **108.17 µs** | 168.92 µs (56.2% slower) | 1.37 ms (1167.4% slower) | 17.45 ms (16031.1% slower) | 11.67 ms (10692.1% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **120.10 µs** | 156.15 µs (30% slower) | 3.38 ms (2711.5% slower) | 460.31 µs (283.3% slower) | 4.02 ms (3249.3% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **384.40 µs** | 739.65 µs (92.4% slower) | 12.03 ms (3029.9% slower) | 5.63 ms (1365.2% slower) | 38.59 ms (9938.5% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.06 ms (40.4% slower) | **2.18 ms** | 31.72 ms (1354.4% slower) | 58.86 ms (2598.6% slower) | 1634.02 ms (74816.3% slower) | DuckDB |
| DELETE throughput | 10 000 | **94.19 µs** | 99.15 µs (5.3% slower) | 4.55 ms (4732.8% slower) | 572.08 µs (507.4% slower) | 1.31 ms (1287.3% slower) | UltraSQL |
| DELETE throughput | 100 000 | **375.04 µs** | 409.29 µs (9.1% slower) | 3.99 ms (962.9% slower) | 7.16 ms (1809.3% slower) | 12.47 ms (3225.3% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.35 ms (25.2% slower) | 4.30 ms (60.7% slower) | **2.68 ms** | 71.09 ms (2554.1% slower) | 300.20 ms (11107.3% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 124.94 µs/op (525.3% slower) | 143.10 µs/op (616.2% slower) | 27.30 ms/op (136520% slower) | **19.98 µs/op** | 32.92 µs/op (64.8% slower) | SQLite |
| Mixed correctness | 100 000 | **145.29 µs** | 265.40 µs (82.7% slower) | 82.70 ms (56823.4% slower) | 2.26 ms (1452.8% slower) | 3.16 ms (2078.3% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.85 ms** | 6.79 ms (40% slower) | 5.42 ms (11.9% slower) | 27.07 ms (458.2% slower) | 15.75 ms (224.8% slower) | UltraSQL |
