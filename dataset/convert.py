import duckdb
import os
import sys

# --- Configuration ---
SOURCE_DIR = './tbl'  # directory file .tbl
OUTPUT_DIR = '.' # folder untuk converted data
TABLES = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']

# Format target: diambil dari argumen CLI (mis. `python convert.py csv`).
# Kalau tidak ada argumen, konversi ke SEMUA format (perilaku lama tetap sama).
# Ini memungkinkan konversi satu-per-satu untuk menghemat ruang disk.
VALID_FORMATS = ['csv', 'jsonl', 'parquet']
if len(sys.argv) > 1:
    TARGET_FORMATS = [f.strip().lower() for f in sys.argv[1:]]
    for f in TARGET_FORMATS:
        if f not in VALID_FORMATS:
            raise ValueError(f"Format '{f}' tidak dikenal. Pilih dari: {VALID_FORMATS}")
else:
    TARGET_FORMATS = VALID_FORMATS

# Defining the TPC-H schemas
SCHEMAS = {
    'lineitem': {
        'l_orderkey': 'BIGINT',
        'l_partkey': 'BIGINT',
        'l_suppkey': 'BIGINT',
        'l_linenumber': 'INTEGER',
        'l_quantity': 'DECIMAL(15, 2)',
        'l_extendedprice': 'DECIMAL(15, 2)',
        'l_discount': 'DECIMAL(15, 2)',
        'l_tax': 'DECIMAL(15, 2)',
        'l_returnflag': 'VARCHAR',
        'l_linestatus': 'VARCHAR',
        'l_shipdate': 'DATE',
        'l_commitdate': 'DATE',
        'l_receiptdate': 'DATE',
        'l_shipinstruct': 'VARCHAR',
        'l_shipmode': 'VARCHAR',
        'l_comment': 'VARCHAR'
    },
    'orders': {
        'o_orderkey': 'BIGINT',
        'o_custkey': 'BIGINT',
        'o_orderstatus': 'VARCHAR',
        'o_totalprice': 'DECIMAL(15, 2)',
        'o_orderdate': 'DATE',
        'o_orderpriority': 'VARCHAR',
        'o_clerk': 'VARCHAR',
        'o_shippriority': 'INTEGER',
        'o_comment': 'VARCHAR'
    },
    'customer': {
        'c_custkey': 'BIGINT',
        'c_name': 'VARCHAR',
        'c_address': 'VARCHAR',
        'c_nationkey': 'BIGINT',
        'c_phone': 'VARCHAR',
        'c_acctbal': 'DECIMAL(15, 2)',
        'c_mktsegment': 'VARCHAR',
        'c_comment': 'VARCHAR'
    },
    'part': {
        'p_partkey': 'BIGINT',
        'p_name': 'VARCHAR',
        'p_mfgr': 'VARCHAR',
        'p_brand': 'VARCHAR',
        'p_type': 'VARCHAR',
        'p_size': 'INTEGER',
        'p_container': 'VARCHAR',
        'p_retailprice': 'DECIMAL(15, 2)',
        'p_comment': 'VARCHAR'
    },
    'supplier': {
        's_suppkey': 'BIGINT',
        's_name': 'VARCHAR',
        's_address': 'VARCHAR',
        's_nationkey': 'BIGINT',
        's_phone': 'VARCHAR',
        's_acctbal': 'DECIMAL(15, 2)',
        's_comment': 'VARCHAR'
    },
    'partsupp': {
        'ps_partkey': 'BIGINT',
        'ps_suppkey': 'BIGINT',
        'ps_availqty': 'INTEGER',
        'ps_supplycost': 'DECIMAL(15, 2)',
        'ps_comment': 'VARCHAR'
    },
    'nation': {
        'n_nationkey': 'BIGINT',
        'n_name': 'VARCHAR',
        'n_regionkey': 'BIGINT',
        'n_comment': 'VARCHAR'
    },
    'region': {
        'r_regionkey': 'BIGINT',
        'r_name': 'VARCHAR',
        'r_comment': 'VARCHAR'
    }
}

# Create directories only for the requested output formats
for fmt in TARGET_FORMATS:
    os.makedirs(f'{OUTPUT_DIR}/{fmt}', exist_ok=True)

print(f"Starting conversion for 8 tables with DuckDB (formats: {', '.join(TARGET_FORMATS)})...")

con = duckdb.connect()

# cek apakah table ada di SCHEMAS
for table in TABLES:
    if table not in SCHEMAS:
        print(f"Skipping {table}: No schema defined.")
        continue

    print(f'Processing {table} with explicit schema...')

    tbl_file = f'{SOURCE_DIR}/{table}.tbl'
    csv_file = f'{OUTPUT_DIR}/csv/{table}.csv'
    jsonl_file = f'{OUTPUT_DIR}/jsonl/{table}.jsonl'
    parquet_file = f'{OUTPUT_DIR}/parquet/{table}.parquet'

    table_schema = SCHEMAS[table]

    # Build the 'read_csv' SQL
    read_tbl_sql = f"""
        read_csv(
            '{tbl_file}',
            delim='|',
            header=False,
            names={list(table_schema.keys())},
            types={list(table_schema.values())}
        )
    """

    # We now select all columns, as there is no '_unused' to exclude
    select_sql = f"SELECT * FROM {read_tbl_sql}"

    # 1. Copy to CSV
    if 'csv' in TARGET_FORMATS:
        con.execute(f"COPY ({select_sql}) TO '{csv_file}' (FORMAT 'CSV', HEADER 1)")

    # 2. Copy to JSONL
    if 'jsonl' in TARGET_FORMATS:
        con.execute(f"COPY ({select_sql}) TO '{jsonl_file}' (FORMAT 'JSON')")

    # 3. Copy to Parquet
    if 'parquet' in TARGET_FORMATS:
        con.execute(f"COPY ({select_sql}) TO '{parquet_file}' (FORMAT 'PARQUET', COMPRESSION 'SNAPPY')")

    print(f"Finished {table}.")

print("All conversions complete!")