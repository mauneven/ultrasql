## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | 8.61 ms (118.5% slower) | 65.64 ms (1566% slower) | 62.13 ms (1476.7% slower) | 23.67 ms (500.7% slower) | **3.94 ms** | PostgreSQL |
| INSERT throughput | 100 000 | **17.50 ms** | 400.00 ms (2185.4% slower) | 655.13 ms (3643% slower) | 45.68 ms (161% slower) | 22.98 ms (31.3% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | - | 3786.52 ms (1271.2% slower) | 6484.75 ms (2248.3% slower) | 280.12 ms (1.4% slower) | **276.15 ms** | PostgreSQL |
| SELECT scan | 10 000 | **519.35 µs** | 866.29 µs (66.8% slower) | 1.01 ms (94.8% slower) | 1.82 ms (250.8% slower) | 1.40 ms (169.6% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.45 ms** | 9.75 ms (51.2% slower) | 6.85 ms (6.3% slower) | 19.83 ms (207.6% slower) | 15.70 ms (143.5% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **52.19 ms** | 94.65 ms (81.3% slower) | 66.91 ms (28.2% slower) | 209.14 ms (300.7% slower) | 171.13 ms (227.9% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **38.75 µs** | 70.00 µs (80.6% slower) | 436.67 µs (1026.9% slower) | 142.23 µs (267% slower) | 280.19 µs (623.1% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **56.92 µs** | 87.88 µs (54.4% slower) | 727.23 µs (1177.7% slower) | 1.39 ms (2344.8% slower) | 2.33 ms (3995% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **37.67 µs** | 178.71 µs (374.4% slower) | 1.69 ms (4379.2% slower) | 16.77 ms (44425.6% slower) | 11.55 ms (30575.1% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **39.73 µs** | 70.94 µs (78.6% slower) | 479.29 µs (1106.4% slower) | 143.37 µs (260.9% slower) | 305.81 µs (669.7% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **37.79 µs** | 115.02 µs (204.4% slower) | 733.88 µs (1841.9% slower) | 1.40 ms (3598.6% slower) | 2.56 ms (6676.1% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **37.35 µs** | 233.31 µs (524.6% slower) | 1.68 ms (4403.5% slower) | 16.32 ms (43589.2% slower) | 12.41 ms (33119.8% slower) | UltraSQL |
| Filter + SUM | 10 000 | **38.96 µs** | 82.19 µs (111% slower) | 617.75 µs (1485.7% slower) | 151.04 µs (287.7% slower) | 302.98 µs (677.7% slower) | UltraSQL |
| Filter + SUM | 100 000 | **39.06 µs** | 128.21 µs (228.2% slower) | 771.42 µs (1874.8% slower) | 1.55 ms (3869.3% slower) | 2.54 ms (6392.9% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **35.23 µs** | 168.60 µs (378.6% slower) | 1.47 ms (4073.6% slower) | 17.93 ms (50788.9% slower) | 12.29 ms (34774.7% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **117.42 µs** | 158.44 µs (34.9% slower) | 3.78 ms (3115.8% slower) | 473.27 µs (303.1% slower) | 4.90 ms (4075.2% slower) | UltraSQL |
| UPDATE throughput | 100 000 | 745.35 µs (0.7% slower) | **739.90 µs** | 11.77 ms (1490.5% slower) | 5.48 ms (640.9% slower) | 103.18 ms (13845.4% slower) | DuckDB |
| UPDATE throughput | 1 000 000 | 6.94 ms (215.8% slower) | **2.20 ms** | 60.99 ms (2674.6% slower) | 60.88 ms (2669.3% slower) | 1838.86 ms (83550.4% slower) | DuckDB |
| DELETE throughput | 10 000 | **96.23 µs** | 103.00 µs (7% slower) | 4.88 ms (4968.7% slower) | 569.94 µs (492.3% slower) | 1.60 ms (1558.2% slower) | UltraSQL |
| DELETE throughput | 100 000 | 532.67 µs (26.8% slower) | **420.21 µs** | 3.85 ms (815.5% slower) | 6.98 ms (1561.7% slower) | 13.97 ms (3225.4% slower) | DuckDB |
| DELETE throughput | 1 000 000 | 4.68 ms (52% slower) | 4.39 ms (42.6% slower) | **3.08 ms** | 77.93 ms (2432.6% slower) | 387.63 ms (12497.6% slower) | ClickHouse |
| Mixed OLTP | 10 000 | 416.75 µs/op (1386.5% slower) | 150.39 µs/op (436.4% slower) | 29.22 ms/op (104114.5% slower) | **28.04 µs/op** | 29.02 µs/op (3.5% slower) | SQLite |
| Mixed correctness | 100 000 | **170.65 µs** | 265.54 µs (55.6% slower) | 77.29 ms (45189.7% slower) | 2.20 ms (1190.9% slower) | 3.17 ms (1760.1% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.40 ms** | 7.13 ms (62% slower) | 5.89 ms (33.9% slower) | 27.85 ms (533% slower) | 16.08 ms (265.4% slower) | UltraSQL |
