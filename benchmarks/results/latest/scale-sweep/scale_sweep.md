## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **1.49 ms** | 64.62 ms (4233.7% slower) | 61.66 ms (4035.3% slower) | 21.56 ms (1346% slower) | 3.63 ms (143.4% slower) | UltraSQL |
| INSERT throughput | 100 000 | **10.34 ms** | 395.09 ms (3721.1% slower) | 650.10 ms (6187.5% slower) | 42.78 ms (313.8% slower) | 23.09 ms (123.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **108.26 ms** | 3745.77 ms (3359.8% slower) | 6508.21 ms (5911.4% slower) | 267.22 ms (146.8% slower) | 255.59 ms (136.1% slower) | UltraSQL |
| SELECT scan | 10 000 | **568.17 µs** | 873.17 µs (53.7% slower) | 944.17 µs (66.2% slower) | 1.84 ms (223% slower) | 1.42 ms (150% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.90 ms** | 9.01 ms (52.6% slower) | 6.46 ms (9.4% slower) | 19.20 ms (225.1% slower) | 15.30 ms (159.2% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **58.12 ms** | 90.96 ms (56.5% slower) | 59.86 ms (3% slower) | 204.12 ms (251.2% slower) | 158.65 ms (173% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **31.12 µs** | 68.48 µs (120% slower) | 442.19 µs (1320.7% slower) | 136.21 µs (337.6% slower) | 269.15 µs (764.7% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **38.38 µs** | 86.25 µs (124.8% slower) | 654.39 µs (1605.3% slower) | 1.40 ms (3546.5% slower) | 2.35 ms (6013.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **62.85 µs** | 161.29 µs (156.6% slower) | 1.62 ms (2479.8% slower) | 15.70 ms (24873.7% slower) | 11.14 ms (17615.7% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **31.40 µs** | 73.75 µs (134.9% slower) | 472.54 µs (1405.1% slower) | 136.52 µs (334.8% slower) | 292.75 µs (832.5% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **32.81 µs** | 116.42 µs (254.8% slower) | 690.85 µs (2005.4% slower) | 1.42 ms (4214.7% slower) | 2.54 ms (7647.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **43.00 µs** | 236.77 µs (450.6% slower) | 1.64 ms (3705.7% slower) | 15.62 ms (36232.6% slower) | 11.95 ms (27696.5% slower) | UltraSQL |
| Filter + SUM | 10 000 | **39.42 µs** | 80.92 µs (105.3% slower) | 531.71 µs (1248.9% slower) | 152.50 µs (286.9% slower) | 317.50 µs (705.5% slower) | UltraSQL |
| Filter + SUM | 100 000 | **38.71 µs** | 129.79 µs (235.3% slower) | 777.56 µs (1908.8% slower) | 1.55 ms (3913.4% slower) | 2.57 ms (6532.6% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **37.69 µs** | 163.60 µs (334.1% slower) | 1.38 ms (3573.2% slower) | 17.83 ms (47209.4% slower) | 11.70 ms (30944% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **111.54 µs** | 159.29 µs (42.8% slower) | 3.58 ms (3111.4% slower) | 480.96 µs (331.2% slower) | 4.78 ms (4186.1% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **339.17 µs** | 767.02 µs (126.1% slower) | 11.27 ms (3222.2% slower) | 5.46 ms (1509.5% slower) | 99.16 ms (29135.7% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 3.16 ms (47.5% slower) | **2.15 ms** | 34.45 ms (1505.6% slower) | 61.69 ms (2774.8% slower) | 1812.10 ms (84345.8% slower) | DuckDB |
| DELETE throughput | 10 000 | **96.62 µs** | 99.77 µs (3.3% slower) | 4.66 ms (4723% slower) | 576.50 µs (496.6% slower) | 1.52 ms (1469.1% slower) | UltraSQL |
| DELETE throughput | 100 000 | **322.10 µs** | 407.25 µs (26.4% slower) | 3.62 ms (1025.4% slower) | 6.87 ms (2031.4% slower) | 13.58 ms (4117.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | 3.40 ms (26.2% slower) | 4.25 ms (57.6% slower) | **2.70 ms** | 73.80 ms (2635.3% slower) | 391.89 ms (14425.5% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 28.80 µs/op (87% slower) | 147.16 µs/op (855.8% slower) | 23.26 ms/op (150937.4% slower) | **15.40 µs/op** | 28.16 µs/op (82.9% slower) | SQLite |
| Mixed correctness | 100 000 | **64.21 µs** | 270.71 µs (321.6% slower) | 75.96 ms (118199.2% slower) | 2.24 ms (3389.5% slower) | 3.17 ms (4833% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.59 ms** | 6.94 ms (51.1% slower) | 5.58 ms (21.4% slower) | 27.79 ms (505% slower) | 15.84 ms (244.8% slower) | UltraSQL |
