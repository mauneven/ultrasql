## Release-artifact scale sweep

UltraSQL is an external release binary launched as ultrasqld; measured engines use installed local clients on the same host.

| Workload | Rows | UltraSQL | DuckDB | ClickHouse | SQLite | PostgreSQL | Fastest |
|---|---:|---:|---:|---:|---:|---:|---|
| INSERT throughput | 10 000 | **3.62 ms** | 66.89 ms (1750.3% slower) | 62.19 ms (1620.2% slower) | 20.46 ms (465.9% slower) | 56.74 ms (1469.6% slower) | UltraSQL |
| INSERT throughput | 100 000 | **30.45 ms** | 420.17 ms (1279.7% slower) | 652.68 ms (2043.2% slower) | 64.96 ms (113.3% slower) | 224.33 ms (636.6% slower) | UltraSQL |
| INSERT throughput | 1 000 000 | **337.07 ms** | 4022.23 ms (1093.3% slower) | 6495.02 ms (1826.9% slower) | 693.85 ms (105.8% slower) | 2460.03 ms (629.8% slower) | UltraSQL |
| SELECT scan | 10 000 | **624.29 µs** | 876.58 µs (40.4% slower) | 997.52 µs (59.8% slower) | 1.86 ms (198.4% slower) | 30.32 ms (4756.8% slower) | UltraSQL |
| SELECT scan | 100 000 | **6.52 ms** | 9.84 ms (50.8% slower) | 7.10 ms (8.9% slower) | 19.88 ms (204.9% slower) | 59.25 ms (808.7% slower) | UltraSQL |
| SELECT scan | 1 000 000 | **62.31 ms** | 96.82 ms (55.4% slower) | 63.82 ms (2.4% slower) | 206.19 ms (230.9% slower) | 212.48 ms (241% slower) | UltraSQL |
| SELECT SUM(x) | 10 000 | **59.33 µs** | 71.48 µs (20.5% slower) | 460.38 µs (675.9% slower) | 137.90 µs (132.4% slower) | 26.66 ms (44832.3% slower) | UltraSQL |
| SELECT SUM(x) | 100 000 | **43.83 µs** | 104.42 µs (138.2% slower) | 695.62 µs (1487% slower) | 1.45 ms (3214.9% slower) | 38.57 ms (87902% slower) | UltraSQL |
| SELECT SUM(x) | 1 000 000 | **56.12 µs** | 170.98 µs (204.6% slower) | 1.69 ms (2908% slower) | 14.30 ms (25378.4% slower) | 46.33 ms (82448.3% slower) | UltraSQL |
| SELECT AVG(x) | 10 000 | **46.38 µs** | 91.73 µs (97.8% slower) | 489.96 µs (956.5% slower) | 137.25 µs (196% slower) | 27.00 ms (58111.1% slower) | UltraSQL |
| SELECT AVG(x) | 100 000 | **51.33 µs** | 129.50 µs (152.3% slower) | 768.46 µs (1397% slower) | 1.45 ms (2731.1% slower) | 39.68 ms (77207.3% slower) | UltraSQL |
| SELECT AVG(x) | 1 000 000 | **44.21 µs** | 250.94 µs (467.6% slower) | 1.70 ms (3748.4% slower) | 14.21 ms (32041.2% slower) | 46.63 ms (105381.9% slower) | UltraSQL |
| Filter + SUM | 10 000 | **57.38 µs** | 77.54 µs (35.1% slower) | 556.38 µs (869.7% slower) | 152.83 µs (166.4% slower) | 26.78 ms (46582.1% slower) | UltraSQL |
| Filter + SUM | 100 000 | **62.96 µs** | 136.38 µs (116.6% slower) | 823.04 µs (1207.3% slower) | 1.61 ms (2458.4% slower) | 39.74 ms (63028.3% slower) | UltraSQL |
| Filter + SUM | 1 000 000 | **63.37 µs** | 164.50 µs (159.6% slower) | 1.59 ms (2409.9% slower) | 15.98 ms (25113.3% slower) | 45.53 ms (71746.2% slower) | UltraSQL |
| UPDATE throughput | 10 000 | **128.75 µs** | 156.96 µs (21.9% slower) | 4.01 ms (3018% slower) | 425.81 µs (230.7% slower) | 50.17 ms (38867.5% slower) | UltraSQL |
| UPDATE throughput | 100 000 | **532.21 µs** | 781.04 µs (46.8% slower) | 12.66 ms (2279.7% slower) | 4.25 ms (698.7% slower) | 183.33 ms (34346.7% slower) | UltraSQL |
| UPDATE throughput | 1 000 000 | **1.88 ms** | 2.31 ms (22.6% slower) | 33.83 ms (1699.3% slower) | 44.05 ms (2242.9% slower) | 2148.27 ms (114154.4% slower) | UltraSQL |
| DELETE throughput | 10 000 | **153.33 µs** | 2.10 ms (1269.2% slower) | 4.94 ms (3119.1% slower) | 536.88 µs (250.1% slower) | 23.48 ms (15216.2% slower) | UltraSQL |
| DELETE throughput | 100 000 | **552.25 µs** | 20.24 ms (3565.8% slower) | 3.76 ms (580.1% slower) | 5.90 ms (968.9% slower) | 40.17 ms (7174.5% slower) | UltraSQL |
| DELETE throughput | 1 000 000 | **2.55 ms** | 229.94 ms (8907.6% slower) | 2.90 ms (13.5% slower) | 60.23 ms (2259.3% slower) | 171.47 ms (6617.3% slower) | UltraSQL |
| Mixed OLTP | 10 000 | **207.35 µs/op** | 1.30 ms/op (527% slower) | 26.31 ms/op (12588.8% slower) | 375.17 µs/op (80.9% slower) | 12.27 ms/op (5817.6% slower) | UltraSQL |
| Mixed correctness | 100 000 | **155.46 µs** | 282.46 µs (81.7% slower) | 80.64 ms (51772% slower) | 2.23 ms (1332.2% slower) | 3.67 ms (2258.7% slower) | UltraSQL |
| Window row_number() | 65 536 | **4.44 ms** | 7.35 ms (65.6% slower) | 6.15 ms (38.5% slower) | 30.52 ms (587.2% slower) | 61.19 ms (1277.8% slower) | UltraSQL |
