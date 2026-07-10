"""
Ekstrak metrik ANDAL dari Spark event log (logs/spark-logs/), untuk SF5 & SF10.
Metrik ini berasal dari akumulator Spark sendiri (bukan sampler ps yang rusak),
sehingga layak dipakai untuk analisis sumber daya di 4.3.2 / 4.4.2.

Output: ringkasan per (scale, arch, format) rata-rata 3 run.
Jalankan: ./.venv/bin/python scripts/parse_spark_metrics.py
"""
import zstandard, json, glob, re, statistics as st
from collections import defaultdict

APP_RE = re.compile(r"TPCH_Query9_(?P<arch>ETL|ELT|HYBRID|Hybrid)_(?P<fmt>csv|jsonl|parquet)_(?P<run>run\d+)_(?P<scale>sf\d+)")

def parse_app(path):
    dctx = zstandard.ZstdDecompressor()
    with open(path, "rb") as fh:
        data = dctx.stream_reader(fh).read().decode("utf-8", "replace")
    app = dict(cpu_ns=0, run_ms=0, gc_ms=0, peak_mem=0, mem_spill=0, disk_spill=0,
               input_b=0, sh_read=0, sh_write=0, tasks=0, name=None)
    for line in data.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        ev = e.get("Event", "")
        if ev.endswith("ApplicationStart"):
            app["name"] = e.get("App Name")
        elif ev.endswith("TaskEnd"):
            tm = e.get("Task Metrics")
            if not tm:
                continue
            app["tasks"] += 1
            app["cpu_ns"] += tm.get("Executor CPU Time", 0)
            app["run_ms"] += tm.get("Executor Run Time", 0)
            app["gc_ms"] += tm.get("JVM GC Time", 0)
            app["peak_mem"] = max(app["peak_mem"], tm.get("Peak Execution Memory", 0))
            app["mem_spill"] += tm.get("Memory Bytes Spilled", 0)
            app["disk_spill"] += tm.get("Disk Bytes Spilled", 0)
            app["input_b"] += tm.get("Input Metrics", {}).get("Bytes Read", 0)
            app["sh_read"] += tm.get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0) + \
                              tm.get("Shuffle Read Metrics", {}).get("Local Bytes Read", 0)
            app["sh_write"] += tm.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
    return app

# Kumpulkan per (scale, arch, fmt) -> list of per-run metric dicts
scen = defaultdict(list)
for path in sorted(glob.glob("logs/spark-logs/eventlog_v2_local-*/events_1_*.zstd")):
    a = parse_app(path)
    if not a["name"]:
        continue
    m = APP_RE.match(a["name"])
    if not m:
        continue
    arch = m.group("arch").capitalize().replace("Hybrid", "Hibrida")
    if arch == "Elt": arch = "ELT"
    if arch == "Etl": arch = "ETL"
    key = (m.group("scale"), arch, m.group("fmt"))
    a["run"] = m.group("run")
    scen[key].append(a)

def mean(vals):
    return st.mean(vals) if vals else 0

print(f"{'scale':5} {'arch':7} {'fmt':8} {'nrun':4} {'CPUs':>8} {'GCs':>6} {'peakMemMB':>10} {'spillMB':>8} {'inputMB':>9} {'shuffMB':>8}")
rows = {}
for key in sorted(scen):
    scale, arch, fmt = key
    runs = scen[key]
    # dedup: kalau ada >3 run (mis. attempt gagal), pakai run dgn task terbanyak per run-id
    byrun = {}
    for a in runs:
        r = a["run"]
        if r not in byrun or a["tasks"] > byrun[r]["tasks"]:
            byrun[r] = a
    runs = list(byrun.values())
    cpu_s = mean([a["cpu_ns"]/1e9 for a in runs])
    gc_s = mean([a["gc_ms"]/1000 for a in runs])
    peak = mean([a["peak_mem"]/1024/1024 for a in runs])
    spill = mean([(a["mem_spill"]+a["disk_spill"])/1024/1024 for a in runs])
    inp = mean([a["input_b"]/1024/1024 for a in runs])
    shuf = mean([(a["sh_read"]+a["sh_write"])/1024/1024 for a in runs])
    rows[key] = dict(cpu_s=cpu_s, gc_s=gc_s, peak=peak, spill=spill, inp=inp, shuf=shuf, nrun=len(runs))
    print(f"{scale:5} {arch:7} {fmt:8} {len(runs):4} {cpu_s:8.1f} {gc_s:6.1f} {peak:10.1f} {spill:8.1f} {inp:9.1f} {shuf:8.1f}")
