## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **5.21 ms** | 66.42 ms (1174.4% slower) | 65.69 ms (1160.4% slower) | 20.14 ms (286.4% slower) | 52.97 ms (916.3% slower) | UltraSQL |
| INSERT throughput | 100 000 | **47.03 ms** | 418.06 ms (789% slower) | 641.93 ms (1265.1% slower) | 64.60 ms (37.4% slower) | 202.91 ms (331.5% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **491.91 ms** | 3980.24 ms (709.1% slower) | 6530.64 ms (1227.6% slower) | 667.36 ms (35.7% slower) | 2183.92 ms (344% slower) | UltraSQL |
| SELECT scan | 10 000 | **538.71 µs** | 862.52 µs (60.1% slower) | 1.06 ms (96.3% slower) | 1.89 ms (250.2% slower) | 27.60 ms (5024% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.23 ms** | 10.01 ms (60.6% slower) | 7.48 ms (20% slower) | 20.34 ms (226.5% slower) | 59.12 ms (848.8% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **59.96 ms** | 93.24 ms (55.5% slower) | 65.42 ms (9.1% slower) | 202.02 ms (236.9% slower) | 210.43 ms (251% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **50.79 µs** | 88.75 µs (74.7% slower) | 503.54 µs (891.4% slower) | 140.98 µs (177.6% slower) | 24.69 ms (48512% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **35.00 µs** | 101.10 µs (188.9% slower) | 732.48 µs (1992.8% slower) | 1.41 ms (3936% slower) | 39.46 ms (112644.5% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **62.17 µs** | 167.12 µs (168.8% slower) | 1.78 ms (2769.5% slower) | 14.18 ms (22712.2% slower) | 41.93 ms (67346.4% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **55.08 µs** | 96.38 µs (75% slower) | 464.81 µs (743.8% slower) | 140.02 µs (154.2% slower) | 25.55 ms (46278.1% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **60.58 µs** | 128.29 µs (111.8% slower) | 769.31 µs (1169.8% slower) | 1.42 ms (2243.7% slower) | 39.24 ms (64674.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **51.67 µs** | 266.00 µs (414.8% slower) | 1.77 ms (3320.3% slower) | 14.48 ms (27924.9% slower) | 42.66 ms (82464.1% slower) | UltraSQL |
| Filter + SUM | 10 000 | **42.83 µs** | 104.02 µs (142.8% slower) | 603.96 µs (1310% slower) | 155.54 µs (263.1% slower) | 24.61 ms (57359.3% slower) | UltraSQL |
| Filter + SUM | 100 000 | **70.00 µs** | 150.83 µs (115.5% slower) | 866.27 µs (1137.5% slower) | 1.60 ms (2183.4% slower) | 39.27 ms (56002% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **63.46 µs** | 203.85 µs (221.2% slower) | 1.52 ms (2289.5% slower) | 16.09 ms (25251.5% slower) | 42.83 ms (67386.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **105.58 µs** | 174.69 µs (65.4% slower) | 3.76 ms (3463.6% slower) | 420.42 µs (298.2% slower) | 45.86 ms (43337.7% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **527.12 µs** | 786.96 µs (49.3% slower) | 11.29 ms (2042.4% slower) | 4.21 ms (699.1% slower) | 168.08 ms (31786.3% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.62 ms** | 2.32 ms (43.1% slower) | 34.60 ms (2038.7% slower) | 43.77 ms (2605.5% slower) | 1997.97 ms (123393.5% slower) | UltraSQL |
| DELETE throughput | 10 000 | **141.00 µs** | 2.14 ms (1418.8% slower) | 4.97 ms (3421.4% slower) | 527.71 µs (274.3% slower) | 23.97 ms (16897.5% slower) | UltraSQL |
| DELETE throughput | 100 000 | **519.83 µs** | 20.11 ms (3768.6% slower) | 3.77 ms (625.3% slower) | 5.86 ms (1027.2% slower) | 38.32 ms (7271.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.30 ms** | 208.48 ms (8960.8% slower) | 2.65 ms (15.3% slower) | 59.24 ms (2474.6% slower) | 180.28 ms (7735.1% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **170.48 µs/op** | 1.23 ms/op (620% slower) | 25.72 ms/op (14986% slower) | 325.49 µs/op (90.9% slower) | 10.75 ms/op (6207.4% slower) | UltraSQL |
| Mixed correctness | 100 000 | **141.92 µs** | 268.29 µs (89% slower) | 80.11 ms (56349.8% slower) | 2.22 ms (1467% slower) | 3.69 ms (2502.6% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.13 ms** | 6.60 ms (60% slower) | 5.23 ms (26.7% slower) | 29.04 ms (603.6% slower) | 52.28 ms (1166.7% slower) | UltraSQL |
