## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | 9.58 ms (25.7% slower) | 65.48 ms (759.1% slower) | 65.33 ms (757.1% slower) | 21.91 ms (187.4% slower) | **7.62 ms** | PostgreSQL |
| INSERT throughput | 100 000 | **38.26 ms** | 407.03 ms (963.9% slower) | 639.92 ms (1572.7% slower) | 48.82 ms (27.6% slower) | 48.63 ms (27.1% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | - | 3849.00 ms (1211.6% slower) | 6486.97 ms (2110.5% slower) | **293.47 ms** | 349.97 ms (19.3% slower) | SQLite |
| SELECT scan | 10 000 | **507.31 µs** | 869.02 µs (71.3% slower) | 984.98 µs (94.2% slower) | 1.85 ms (264.3% slower) | 1.48 ms (192% slower) | UltraSQL |
| SELECT scan | 100 000 | 6.39 ms (3% slower) | 9.31 ms (50% slower) | **6.20 ms** | 19.50 ms (214.5% slower) | 15.47 ms (149.4% slower) | ClickHouse |
| SELECT scan | 1 000 000 | **50.07 ms** | 98.71 ms (97.1% slower) | 59.62 ms (19.1% slower) | 206.30 ms (312% slower) | 159.09 ms (217.7% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **32.75 µs** | 70.40 µs (114.9% slower) | 385.38 µs (1076.7% slower) | 143.63 µs (338.6% slower) | 281.98 µs (761% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **32.42 µs** | 94.23 µs (190.7% slower) | 701.79 µs (2064.9% slower) | 1.43 ms (4313.3% slower) | 2.40 ms (7296.4% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **60.23 µs** | 159.62 µs (165% slower) | 1.56 ms (2497.3% slower) | 15.42 ms (25504.1% slower) | 10.68 ms (17625.2% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **32.67 µs** | 76.75 µs (135% slower) | 415.85 µs (1173% slower) | 142.90 µs (337.4% slower) | 298.71 µs (814.4% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **33.65 µs** | 130.44 µs (287.7% slower) | 717.02 µs (2031.1% slower) | 1.46 ms (4252.6% slower) | 2.71 ms (7940.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **37.62 µs** | 222.40 µs (491.1% slower) | 1.57 ms (4074.4% slower) | 15.58 ms (41318.1% slower) | 11.73 ms (31063.2% slower) | UltraSQL |
| Filter + SUM | 10 000 | **35.98 µs** | 79.25 µs (120.3% slower) | 561.02 µs (1459.3% slower) | 156.17 µs (334% slower) | 297.75 µs (727.6% slower) | UltraSQL |
| Filter + SUM | 100 000 | **31.96 µs** | 119.31 µs (273.3% slower) | 927.69 µs (2802.8% slower) | 1.58 ms (4846.3% slower) | 2.58 ms (7974.4% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **37.52 µs** | 169.35 µs (351.4% slower) | 1.53 ms (3974.4% slower) | 17.66 ms (46967.5% slower) | 11.18 ms (29687.2% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **115.94 µs** | 162.79 µs (40.4% slower) | 3.55 ms (2960.8% slower) | 480.77 µs (314.7% slower) | 5.32 ms (4489.7% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **712.98 µs** | 765.31 µs (7.3% slower) | 18.43 ms (2485.3% slower) | 5.71 ms (700.6% slower) | 126.99 ms (17711.1% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | 6.56 ms (213.9% slower) | **2.09 ms** | 53.33 ms (2452.4% slower) | 63.35 ms (2932.1% slower) | 2102.10 ms (100513% slower) | DuckDB |
| DELETE throughput | 10 000 | **87.69 µs** | 101.56 µs (15.8% slower) | 4.65 ms (5204.2% slower) | 584.67 µs (566.8% slower) | 1.65 ms (1777.6% slower) | UltraSQL |
| DELETE throughput | 100 000 | 545.83 µs (31% slower) | **416.69 µs** | 3.21 ms (669.2% slower) | 6.95 ms (1569.1% slower) | 14.12 ms (3288.7% slower) | DuckDB |
| DELETE throughput | 1 000 000 | 4.36 ms (31.7% slower) | 4.41 ms (33% slower) | **3.31 ms** | 78.51 ms (2270.7% slower) | 643.58 ms (19333.8% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 471.76 µs/op (1416.4% slower) | 151.27 µs/op (386.2% slower) | 28.96 ms/op (92986.4% slower) | 38.31 µs/op (23.1% slower) | **31.11 µs/op** | PostgreSQL |
| Mixed correctness | 100 000 | **217.02 µs** | 265.27 µs (22.2% slower) | 72.78 ms (33436.3% slower) | 2.25 ms (938.7% slower) | 3.23 ms (1388.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.52 ms** | 7.53 ms (66.5% slower) | 6.59 ms (45.6% slower) | 28.49 ms (529.7% slower) | 16.84 ms (272.1% slower) | UltraSQL |
