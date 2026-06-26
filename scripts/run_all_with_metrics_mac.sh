#!/bin/bash
set -e

# =========================
# CONFIG
# =========================
PROJECT_ROOT="$HOME/Documents/personal/etl-elt-performance-benchmark"
DB_NAME="tpch_dw"

ETL_SCRIPT="$PROJECT_ROOT/src/etl/etl_query9.py"
ELT_SCRIPT="$PROJECT_ROOT/src/elt/elt_query9.py"
HYBRID_SCRIPT="$PROJECT_ROOT/src/hybrid/hybrid_query9.py"
DBT_DIR="$PROJECT_ROOT/dbt_transform"

LOG_DIR="$PROJECT_ROOT/logs/overnight"
METRICS_FILE="$PROJECT_ROOT/logs/metrics/scenario_times.csv"

FORMATS=("csv" "jsonl" "parquet")
RUNS=(1 2 3)
SCALE_FACTOR="sf5"

JAVA_HOME_PATH="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
GTIME="$(brew --prefix gnu-time)/bin/gtime"

RESOURCE_SAMPLE_FILE="$PROJECT_ROOT/logs/metrics/resource_samples.csv"
SAMPLE_INTERVAL=2

mkdir -p "$LOG_DIR"
mkdir -p "$(dirname "$METRICS_FILE")"

echo "timestamp,scale_factor,architecture,format,run,phase,elapsed_seconds,status,log_file" > "$RESOURCE_SAMPLE_FILE"

# =========================
# HELPER FUNCTION
# =========================
start_resource_monitor() {
    local architecture="$1"
    local format="$2"
    local run="$3"
    local phase="$4"

    (
        while true; do
            local ts
            ts=$(date '+%Y-%m-%d %H:%M:%S')

            local pids
            pids=$(pgrep -f "etl_query9|elt_query9|hybrid_query9|spark|java|postgres|dbt" | tr '\n' ',' | sed 's/,$//')

            if [ -n "$pids" ]; then
                ps -p "$pids" -o pid=,ppid=,comm=,%cpu=,%mem=,rss= | awk \
                    -v ts="$ts" \
                    -v sf="$SCALE_FACTOR" \
                    -v arch="$architecture" \
                    -v fmt="$format" \
                    -v run="$run" \
                    -v phase="$phase" \
                    'BEGIN { OFS="," }
                    {
                        rss_mb = $6 / 1024
                        print ts, sf, arch, fmt, run, phase, $1, $2, $3, $4, $5, rss_mb
                    }' >> "$RESOURCE_SAMPLE_FILE"
            fi

            sleep "$SAMPLE_INTERVAL"
        done
    ) &

    RESOURCE_MONITOR_PID=$!
}

stop_resource_monitor() {
    if [ -n "$RESOURCE_MONITOR_PID" ]; then
        kill "$RESOURCE_MONITOR_PID" 2>/dev/null || true
        wait "$RESOURCE_MONITOR_PID" 2>/dev/null || true
        RESOURCE_MONITOR_PID=""
    fi
}

run_timed() {
    local architecture="$1"
    local format="$2"
    local run="$3"
    local phase="$4"
    local log_file="$5"
    shift 5

    echo ""
    echo "=========================================="
    echo "START: $architecture | $format | run $run | $phase"
    echo "TIME : $(date)"
    echo "=========================================="

    local start_time
    local end_time
    local elapsed
    local status

    start_time=$(python -c "import time; print(time.time())")

    set +e

    start_resource_monitor "$architecture" "$format" "$run" "$phase"

    env JAVA_HOME="$JAVA_HOME_PATH" SPARK_LOCAL_IP="127.0.0.1" \
        "$GTIME" -v -o "${log_file%.log}_resource.txt" \
        "$@" > "$log_file" 2>&1

    status=$?

    stop_resource_monitor

    set -e

    end_time=$(python -c "import time; print(time.time())")

    elapsed=$(python - <<PY
start_time = float("$start_time")
end_time = float("$end_time")
print(round(end_time - start_time, 3))
PY
)

    echo "$(date '+%Y-%m-%d %H:%M:%S'),$SCALE_FACTOR,$architecture,$format,$run,$phase,$elapsed,$status,$log_file" >> "$METRICS_FILE"

    echo "END: $architecture | $format | run $run | $phase"
    echo "ELAPSED: $elapsed seconds"
    echo "STATUS: $status"

    if [ "$status" -ne 0 ]; then
        echo "FAILED. Check log: $log_file"
        exit "$status"
    fi
}

truncate_elt_raw() {
    psql -d "$DB_NAME" -c "
    truncate table
        raw.elt_lineitem,
        raw.elt_nation,
        raw.elt_orders,
        raw.elt_part,
        raw.elt_partsupp,
        raw.elt_supplier
    restart identity cascade;
    "
}

truncate_hybrid_raw() {
    psql -d "$DB_NAME" -c "
    truncate table
        raw.hybrid_lineitem,
        raw.hybrid_nation,
        raw.hybrid_orders,
        raw.hybrid_part,
        raw.hybrid_partsupp,
        raw.hybrid_supplier
    restart identity cascade;
    "
}

cd "$PROJECT_ROOT"

# =========================
# DBT CHECK
# =========================
run_timed "SYSTEM" "none" "0" "dbt_debug" \
    "$LOG_DIR/dbt_debug.log" \
    bash -c "cd '$DBT_DIR' && dbt debug"

# =========================
# RUN ALL SCENARIOS
# =========================
for run in "${RUNS[@]}"; do
    for format in "${FORMATS[@]}"; do

        export FILE_FORMAT_RUN="$format"
        export RUN_ID="run${run}"
        export SCALE_FACTOR="$SCALE_FACTOR"
        export KEEP_UI_OPEN="false"

        echo ""
        echo "##########################################"
        echo "SCENARIO: $SCALE_FACTOR | $format | run $run"
        echo "##########################################"

        # ETL
        run_timed "ETL" "$format" "$run" "spark_etl_total" \
            "$LOG_DIR/etl_${format}_run${run}.log" \
            python -u "$ETL_SCRIPT"

        # ELT
        truncate_elt_raw

        run_timed "ELT" "$format" "$run" "spark_extract_load" \
            "$LOG_DIR/elt_load_${format}_run${run}.log" \
            python -u "$ELT_SCRIPT"

        run_timed "ELT" "$format" "$run" "dbt_transform" \
            "$LOG_DIR/elt_dbt_${format}_run${run}.log" \
            bash -c "cd '$DBT_DIR' && dbt run --select path:models/elt --target-path target/elt_${format}_run${run}"

        # Hybrid
        truncate_hybrid_raw

        run_timed "Hybrid" "$format" "$run" "spark_extract_clean_load" \
            "$LOG_DIR/hybrid_load_${format}_run${run}.log" \
            python -u "$HYBRID_SCRIPT"

        run_timed "Hybrid" "$format" "$run" "dbt_transform" \
            "$LOG_DIR/hybrid_dbt_${format}_run${run}.log" \
            bash -c "cd '$DBT_DIR' && dbt run --select path:models/hybrid --target-path target/hybrid_${format}_run${run}"

    done
done

echo ""
echo "All scenarios finished."
echo "Metrics saved to: $METRICS_FILE"