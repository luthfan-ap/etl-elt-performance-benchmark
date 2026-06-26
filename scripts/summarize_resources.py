from pathlib import Path
import pandas as pd

project_root = Path(__file__).resolve().parent.parent

input_file = project_root / "logs" / "metrics" / "resource_samples.csv"
output_file = project_root / "logs" / "metrics" / "resource_summary.csv"

df = pd.read_csv(input_file)

if df.empty:
    raise ValueError("resource_samples.csv is empty")

# Convert numeric columns
df["cpu_pct"] = pd.to_numeric(df["cpu_pct"], errors="coerce").fillna(0)
df["mem_pct"] = pd.to_numeric(df["mem_pct"], errors="coerce").fillna(0)
df["rss_mb"] = pd.to_numeric(df["rss_mb"], errors="coerce").fillna(0)

# Aggregate all relevant processes per timestamp
sample_level = (
    df.groupby([
        "timestamp",
        "scale_factor",
        "architecture",
        "format",
        "run",
        "phase"
    ], as_index=False)
    .agg(
        total_cpu_pct=("cpu_pct", "sum"),
        total_rss_mb=("rss_mb", "sum"),
        max_process_rss_mb=("rss_mb", "max")
    )
)

# Summarize per scenario phase
summary = (
    sample_level.groupby([
        "scale_factor",
        "architecture",
        "format",
        "run",
        "phase"
    ], as_index=False)
    .agg(
        avg_cpu_pct=("total_cpu_pct", "mean"),
        max_cpu_pct=("total_cpu_pct", "max"),
        avg_rss_mb=("total_rss_mb", "mean"),
        peak_rss_mb=("total_rss_mb", "max"),
        peak_single_process_rss_mb=("max_process_rss_mb", "max"),
        sample_count=("timestamp", "count")
    )
)

summary.to_csv(output_file, index=False)

print(f"Saved resource summary to: {output_file}")