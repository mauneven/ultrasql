## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **4.73 ms** | 62.55 ms (1221.5% slower) | 61.79 ms (1205.5% slower) | 18.37 ms (288% slower) | 52.15 ms (1001.8% slower) | UltraSQL |
| INSERT throughput | 100 000 | **40.89 ms** | 402.27 ms (883.8% slower) | 660.22 ms (1514.7% slower) | 64.28 ms (57.2% slower) | 208.14 ms (409% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **432.29 ms** | 3887.89 ms (799.4% slower) | 6523.15 ms (1409% slower) | 623.80 ms (44.3% slower) | 2338.39 ms (440.9% slower) | UltraSQL |
| SELECT scan | 10 000 | **605.58 µs** | 878.25 µs (45% slower) | 1.03 ms (70% slower) | 1.90 ms (213% slower) | 27.76 ms (4483.4% slower) | UltraSQL |
| SELECT scan | 100 000 | **5.97 ms** | 9.35 ms (56.6% slower) | 6.69 ms (12% slower) | 19.51 ms (226.8% slower) | 55.36 ms (827.2% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **58.13 ms** | 98.78 ms (69.9% slower) | 62.37 ms (7.3% slower) | 202.73 ms (248.7% slower) | 205.54 ms (253.6% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **67.29 µs** | 70.33 µs (4.5% slower) | 537.06 µs (698.1% slower) | 140.69 µs (109.1% slower) | 24.39 ms (36139.4% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **63.00 µs** | 105.75 µs (67.9% slower) | 670.77 µs (964.7% slower) | 1.44 ms (2179.4% slower) | 35.97 ms (57002.3% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **57.58 µs** | 187.83 µs (226.2% slower) | 1.72 ms (2895% slower) | 14.25 ms (24653.6% slower) | 38.53 ms (66818% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **59.04 µs** | 94.77 µs (60.5% slower) | 512.40 µs (767.9% slower) | 139.27 µs (135.9% slower) | 24.70 ms (41736.3% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **50.08 µs** | 132.25 µs (164.1% slower) | 730.42 µs (1358.4% slower) | 1.47 ms (2829.2% slower) | 39.02 ms (77808.5% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **50.92 µs** | 263.92 µs (418.3% slower) | 1.68 ms (3207% slower) | 14.00 ms (27399% slower) | 41.84 ms (82076.2% slower) | UltraSQL |
| Filter + SUM | 10 000 | **78.67 µs** | 104.96 µs (33.4% slower) | 595.06 µs (656.4% slower) | 161.52 µs (105.3% slower) | 24.74 ms (31347.7% slower) | UltraSQL |
| Filter + SUM | 100 000 | **55.12 µs** | 139.75 µs (153.5% slower) | 909.33 µs (1549.6% slower) | 1.59 ms (2785.2% slower) | 36.44 ms (66009.9% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **64.42 µs** | 204.98 µs (218.2% slower) | 1.52 ms (2254.7% slower) | 15.79 ms (24417.3% slower) | 41.89 ms (64926.4% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **133.75 µs** | 156.02 µs (16.7% slower) | 4.01 ms (2894.7% slower) | 416.96 µs (211.7% slower) | 42.40 ms (31598.8% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **527.12 µs** | 774.77 µs (47% slower) | 11.75 ms (2129.6% slower) | 4.21 ms (697.9% slower) | 180.00 ms (34047.4% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.60 ms** | 2.12 ms (32.6% slower) | 33.43 ms (1989.7% slower) | 43.23 ms (2601.7% slower) | 2025.15 ms (126478.7% slower) | UltraSQL |
| DELETE throughput | 10 000 | **157.08 µs** | 2.09 ms (1227.7% slower) | 5.17 ms (3188.1% slower) | 508.88 µs (224% slower) | 21.65 ms (13681.7% slower) | UltraSQL |
| DELETE throughput | 100 000 | **733.71 µs** | 20.19 ms (2652% slower) | 3.51 ms (378.6% slower) | 5.74 ms (682.1% slower) | 38.88 ms (5199.1% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.35 ms** | 213.66 ms (9005.5% slower) | 2.99 ms (27.5% slower) | 57.83 ms (2364.6% slower) | 168.16 ms (7066.3% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **162.40 µs/op** | 1.25 ms/op (670.3% slower) | 26.97 ms/op (16508.9% slower) | 347.99 µs/op (114.3% slower) | 10.70 ms/op (6486% slower) | UltraSQL |
| Mixed correctness | 100 000 | **153.38 µs** | 312.35 µs (103.7% slower) | 78.74 ms (51239.2% slower) | 2.13 ms (1290.5% slower) | 3.60 ms (2246.9% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.15 ms** | 6.67 ms (60.9% slower) | 5.81 ms (40.2% slower) | 29.27 ms (605.9% slower) | 51.10 ms (1132.5% slower) | UltraSQL |
