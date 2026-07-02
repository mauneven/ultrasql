## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.49 ms** | 64.62 ms (4233.7% slower) | 61.66 ms (4035.3% slower) | 21.56 ms (1346% slower) | 3.63 ms (143.4% slower) | UltraSQL |
| INSERT throughput | 100 000 | **10.34 ms** | 395.09 ms (3721.1% slower) | 650.10 ms (6187.5% slower) | 42.78 ms (313.8% slower) | 23.09 ms (123.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **109.51 ms** | 3742.06 ms (3317.1% slower) | 6463.65 ms (5802.4% slower) | 266.27 ms (143.2% slower) | 259.25 ms (136.7% slower) | UltraSQL |
| SELECT scan | 10 000 | **568.17 µs** | 873.17 µs (53.7% slower) | 944.17 µs (66.2% slower) | 1.84 ms (223% slower) | 1.42 ms (150% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.90 ms** | 9.01 ms (52.6% slower) | 6.46 ms (9.4% slower) | 19.20 ms (225.1% slower) | 15.30 ms (159.2% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **57.78 ms** | 91.22 ms (57.9% slower) | 60.84 ms (5.3% slower) | 201.91 ms (249.5% slower) | 160.07 ms (177.1% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **31.12 µs** | 68.48 µs (120% slower) | 442.19 µs (1320.7% slower) | 136.21 µs (337.6% slower) | 269.15 µs (764.7% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **38.38 µs** | 86.25 µs (124.8% slower) | 654.39 µs (1605.3% slower) | 1.40 ms (3546.5% slower) | 2.35 ms (6013.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **41.92 µs** | 161.52 µs (285.3% slower) | 1.62 ms (3763.3% slower) | 15.86 ms (37725% slower) | 11.23 ms (26682.6% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **31.40 µs** | 73.75 µs (134.9% slower) | 472.54 µs (1405.1% slower) | 136.52 µs (334.8% slower) | 292.75 µs (832.5% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **32.81 µs** | 116.42 µs (254.8% slower) | 690.85 µs (2005.4% slower) | 1.42 ms (4214.7% slower) | 2.54 ms (7647.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **44.08 µs** | 217.56 µs (393.5% slower) | 1.64 ms (3617.2% slower) | 15.71 ms (35542.1% slower) | 12.09 ms (27335.2% slower) | UltraSQL |
| Filter + SUM | 10 000 | **39.42 µs** | 80.92 µs (105.3% slower) | 531.71 µs (1248.9% slower) | 152.50 µs (286.9% slower) | 317.50 µs (705.5% slower) | UltraSQL |
| Filter + SUM | 100 000 | **38.71 µs** | 129.79 µs (235.3% slower) | 777.56 µs (1908.8% slower) | 1.55 ms (3913.4% slower) | 2.57 ms (6532.6% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **43.56 µs** | 172.75 µs (296.6% slower) | 1.44 ms (3206.1% slower) | 17.81 ms (40785.3% slower) | 11.98 ms (27408.5% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **111.54 µs** | 159.29 µs (42.8% slower) | 3.58 ms (3111.4% slower) | 480.96 µs (331.2% slower) | 4.78 ms (4186.1% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **339.17 µs** | 767.02 µs (126.1% slower) | 11.27 ms (3222.2% slower) | 5.46 ms (1509.5% slower) | 99.16 ms (29135.7% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.13 ms (46.2% slower) | **2.14 ms** | 32.90 ms (1435.8% slower) | 59.67 ms (2685.8% slower) | 1809.06 ms (84355.6% slower) | DuckDB |
| DELETE throughput | 10 000 | **96.62 µs** | 99.77 µs (3.3% slower) | 4.66 ms (4723% slower) | 576.50 µs (496.6% slower) | 1.52 ms (1469.1% slower) | UltraSQL |
| DELETE throughput | 100 000 | **322.10 µs** | 407.25 µs (26.4% slower) | 3.62 ms (1025.4% slower) | 6.87 ms (2031.4% slower) | 13.58 ms (4117.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.48 ms (25.3% slower) | 4.39 ms (58.2% slower) | **2.78 ms** | 72.33 ms (2506.3% slower) | 371.92 ms (13302.1% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 28.95 µs/op (75.4% slower) | 150.09 µs/op (809.6% slower) | 29.70 ms/op (179913.6% slower) | **16.50 µs/op** | 28.30 µs/op (71.5% slower) | SQLite |
| Mixed correctness | 100 000 | **45.42 µs** | 269.52 µs (493.4% slower) | 73.22 ms (161118.8% slower) | 2.28 ms (4930.3% slower) | 3.14 ms (6814.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.64 ms** | 7.01 ms (50.9% slower) | 5.80 ms (25% slower) | 27.82 ms (499.4% slower) | 15.96 ms (243.8% slower) | UltraSQL |
