import subprocess
import json
import statistics
import os

def run_ultrasql():
    medians = []
    print("Running UltraSQL stress check...")
    for i in range(20):
        cmd = "cargo run --release -p ultrasql-bench --features='sql-bench' --bin cross_compare_sql -- --workload select-scan --rows 10000 --warmup 2 --iters 24"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        try:
            # The output is a JSON line
            for line in result.stdout.split('\n'):
                if line.strip().startswith('{') and '"engine":"ultrasql"' in line:
                    data = json.loads(line)
                    medians.append(data['median_us'])
                    break
        except Exception as e:
            print(f"Error parsing UltraSQL result: {e}")
        
        print(f"UltraSQL run {i+1}/20: {medians[-1] if len(medians) > i else 'Error'}")
    return medians

def run_duckdb():
    medians = []
    print("Running DuckDB stress check...")
    raw_dir = "/tmp/ultrasql-bench-duckdb-stress"
    os.makedirs(raw_dir, exist_ok=True)
    json_path = f"{raw_dir}/select_scan_10k-duckdb.json"
    
    for i in range(20):
        if os.path.exists(json_path):
            os.remove(json_path)
        cmd = f"N_ITERS=24 RAW_DIR={raw_dir} bash benchmarks/scripts/run_duckdb_writes.sh select_scan_10k"
        subprocess.run(cmd, shell=True, capture_output=True)
        
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    medians.append(data[0]['median_us'])
                else:
                    medians.append(data['median_us'])
        except Exception as e:
            print(f"Error reading DuckDB result: {e}")
        
        print(f"DuckDB run {i+1}/20: {medians[-1] if len(medians) > i else 'Error'}")
    return medians

us_medians = run_ultrasql()
db_medians = run_duckdb()

def report(name, vals):
    if not vals:
        print(f"{name}: No data collected.")
        return
    print(f"\n{name} Results (20 runs):")
    print(f"Min:  {min(vals):.2f} us")
    print(f"Max:  {max(vals):.2f} us")
    print(f"Mean: {statistics.mean(vals):.2f} us")

report("UltraSQL", us_medians)
report("DuckDB", db_medians)

if us_medians and db_medians:
    us_worst = max(us_medians)
    db_best = min(db_medians)
    status = "stayed below" if us_worst < db_best else "did NOT stay below"
    print(f"\nUltraSQL's worst median ({us_worst:.2f} us) {status} DuckDB's best median ({db_best:.2f} us).")
