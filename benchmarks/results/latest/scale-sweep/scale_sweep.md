## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **3.63 ms** | 66.43 ms (1730.4% slower) | 62.67 ms (1626.8% slower) | 20.42 ms (462.6% slower) | 53.39 ms (1371.1% slower) | UltraSQL |
| INSERT throughput | 100 000 | **30.48 ms** | 419.19 ms (1275.1% slower) | 654.99 ms (2048.6% slower) | 64.45 ms (111.4% slower) | 202.50 ms (564.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **339.19 ms** | 4020.90 ms (1085.5% slower) | 6513.22 ms (1820.3% slower) | 696.75 ms (105.4% slower) | 2211.34 ms (552% slower) | UltraSQL |
| SELECT scan | 10 000 | **638.42 µs** | 913.90 µs (43.2% slower) | 1.13 ms (77.5% slower) | 1.91 ms (198.9% slower) | 30.03 ms (4603.2% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.50 ms** | 9.67 ms (48.8% slower) | 7.28 ms (12% slower) | 19.79 ms (204.6% slower) | 58.81 ms (804.9% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **62.97 ms** | 98.95 ms (57.2% slower) | 64.81 ms (2.9% slower) | 206.47 ms (227.9% slower) | 214.25 ms (240.2% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **51.96 µs** | 83.58 µs (60.9% slower) | 464.08 µs (793.2% slower) | 138.92 µs (167.4% slower) | 26.49 ms (50875.3% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **45.42 µs** | 102.17 µs (125% slower) | 699.65 µs (1440.5% slower) | 1.47 ms (3128.2% slower) | 38.22 ms (84061.2% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **55.83 µs** | 173.50 µs (210.7% slower) | 1.66 ms (2876.1% slower) | 14.32 ms (25542.9% slower) | 46.92 ms (83931.9% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **40.88 µs** | 83.15 µs (103.4% slower) | 518.98 µs (1169.7% slower) | 146.75 µs (259% slower) | 26.76 ms (65371.4% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **30.62 µs** | 128.33 µs (319% slower) | 708.33 µs (2212.9% slower) | 1.48 ms (4729.3% slower) | 38.89 ms (126886.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **64.42 µs** | 255.31 µs (296.3% slower) | 1.76 ms (2629.2% slower) | 14.29 ms (22079.3% slower) | 45.70 ms (70841.8% slower) | UltraSQL |
| Filter + SUM | 10 000 | **58.42 µs** | 93.98 µs (60.9% slower) | 529.15 µs (805.8% slower) | 152.58 µs (161.2% slower) | 26.68 ms (45565% slower) | UltraSQL |
| Filter + SUM | 100 000 | **36.75 µs** | 133.92 µs (264.4% slower) | 838.52 µs (2181.7% slower) | 1.61 ms (4277.6% slower) | 39.03 ms (106101% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **63.50 µs** | 187.90 µs (195.9% slower) | 1.53 ms (2306.2% slower) | 16.11 ms (25266.7% slower) | 45.23 ms (71128.4% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **127.71 µs** | 177.90 µs (39.3% slower) | 3.68 ms (2784.8% slower) | 436.54 µs (241.8% slower) | 68.96 ms (53894.4% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **537.58 µs** | 800.12 µs (48.8% slower) | 12.04 ms (2140.2% slower) | 4.27 ms (693.7% slower) | 174.10 ms (32286.3% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.65 ms** | 2.21 ms (33.7% slower) | 37.29 ms (2156.8% slower) | 44.21 ms (2575.1% slower) | 2038.36 ms (123247% slower) | UltraSQL |
| DELETE throughput | 10 000 | **151.08 µs** | 2.06 ms (1263.2% slower) | 5.05 ms (3243.4% slower) | 540.46 µs (257.7% slower) | 23.21 ms (15262.7% slower) | UltraSQL |
| DELETE throughput | 100 000 | **541.92 µs** | 20.16 ms (3619.6% slower) | 3.60 ms (564.2% slower) | 5.85 ms (979% slower) | 38.96 ms (7088.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.75 ms** | 228.09 ms (8181.5% slower) | 3.02 ms (9.7% slower) | 60.44 ms (2094.4% slower) | 180.44 ms (6451.3% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **210.78 µs/op** | 1.30 ms/op (517.9% slower) | 27.72 ms/op (13051.5% slower) | 376.00 µs/op (78.4% slower) | 12.20 ms/op (5685.8% slower) | UltraSQL |
| Mixed correctness | 100 000 | **144.42 µs** | 262.42 µs (81.7% slower) | 79.49 ms (54941.8% slower) | 2.24 ms (1449.4% slower) | 3.93 ms (2618.4% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.42 ms** | 7.41 ms (67.6% slower) | 6.11 ms (38.1% slower) | 29.99 ms (578.2% slower) | 58.15 ms (1214.9% slower) | UltraSQL |
