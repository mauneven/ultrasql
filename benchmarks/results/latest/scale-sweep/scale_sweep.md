## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.53 ms** | 66.29 ms (4239.3% slower) | 64.70 ms (4134.8% slower) | 22.37 ms (1364.2% slower) | 3.54 ms (131.6% slower) | UltraSQL |
| INSERT throughput | 100 000 | **9.83 ms** | 401.54 ms (3984.1% slower) | 630.05 ms (6308.3% slower) | 44.43 ms (351.9% slower) | 21.46 ms (118.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **114.43 ms** | 3818.04 ms (3236.7% slower) | 6171.47 ms (5293.5% slower) | 271.30 ms (137.1% slower) | 267.50 ms (133.8% slower) | UltraSQL |
| SELECT scan | 10 000 | **579.71 µs** | 870.27 µs (50.1% slower) | 975.63 µs (68.3% slower) | 1.88 ms (224.4% slower) | 1.46 ms (152.3% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.64 ms** | 9.26 ms (64.2% slower) | 6.60 ms (17% slower) | 19.51 ms (246% slower) | 15.29 ms (171.1% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **57.97 ms** | 91.29 ms (57.5% slower) | 58.77 ms (1.4% slower) | 205.37 ms (254.3% slower) | 158.62 ms (173.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **39.29 µs** | 68.50 µs (74.3% slower) | 491.02 µs (1149.7% slower) | 143.29 µs (264.7% slower) | 298.38 µs (659.4% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **34.98 µs** | 92.35 µs (164% slower) | 669.21 µs (1813.2% slower) | 1.44 ms (4018% slower) | 2.39 ms (6722.9% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **48.58 µs** | 167.33 µs (244.4% slower) | 1.60 ms (3198.4% slower) | 15.50 ms (31808.8% slower) | 10.93 ms (22403% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **28.58 µs** | 73.50 µs (157.1% slower) | 486.52 µs (1602.1% slower) | 143.02 µs (400.4% slower) | 319.37 µs (1017.3% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **39.19 µs** | 114.21 µs (191.4% slower) | 688.12 µs (1656% slower) | 1.44 ms (3579.3% slower) | 2.61 ms (6556.9% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **63.33 µs** | 237.00 µs (274.2% slower) | 1.57 ms (2372.4% slower) | 15.46 ms (24317.5% slower) | 11.75 ms (18455.1% slower) | UltraSQL |
| Filter + SUM | 10 000 | **37.52 µs** | 84.19 µs (124.4% slower) | 578.77 µs (1442.5% slower) | 151.85 µs (304.7% slower) | 321.31 µs (756.4% slower) | UltraSQL |
| Filter + SUM | 100 000 | **36.54 µs** | 123.12 µs (236.9% slower) | 821.33 µs (2147.6% slower) | 1.59 ms (4262.2% slower) | 2.60 ms (7007% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **58.67 µs** | 166.25 µs (183.4% slower) | 1.57 ms (2577.6% slower) | 17.42 ms (29586.8% slower) | 11.76 ms (19937% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **116.40 µs** | 160.19 µs (37.6% slower) | 3.70 ms (3077% slower) | 488.96 µs (320.1% slower) | 5.24 ms (4402.8% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **711.65 µs** | 764.79 µs (7.5% slower) | 11.51 ms (1517.3% slower) | 5.53 ms (677.1% slower) | 107.26 ms (14971.5% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 6.63 ms (202% slower) | **2.20 ms** | 34.28 ms (1461.1% slower) | 59.09 ms (2591.3% slower) | 1806.95 ms (82193.2% slower) | DuckDB |
| DELETE throughput | 10 000 | **97.90 µs** | 103.96 µs (6.2% slower) | 4.18 ms (4166.2% slower) | 586.40 µs (499% slower) | 1.63 ms (1561.4% slower) | UltraSQL |
| DELETE throughput | 100 000 | **313.98 µs** | 422.10 µs (34.4% slower) | 3.30 ms (951.6% slower) | 7.04 ms (2140.9% slower) | 13.80 ms (4296% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 5.40 ms (59.1% slower) | 4.28 ms (26% slower) | **3.40 ms** | 71.88 ms (2015.8% slower) | 431.39 ms (12598.8% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 30.62 µs/op (104.5% slower) | 155.06 µs/op (935.4% slower) | 24.06 ms/op (160526.2% slower) | **14.98 µs/op** | 29.33 µs/op (95.8% slower) | SQLite |
| Mixed correctness | 100 000 | **49.38 µs** | 261.54 µs (429.7% slower) | 76.27 ms (154374.8% slower) | 2.23 ms (4423.1% slower) | 3.18 ms (6347.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.57 ms** | 6.80 ms (48.7% slower) | 5.69 ms (24.6% slower) | 27.46 ms (500.9% slower) | 15.73 ms (244.3% slower) | UltraSQL |
