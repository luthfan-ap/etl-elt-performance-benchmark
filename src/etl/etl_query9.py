# Import dependencies
import os
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime


# Load environment variables
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# List of table names
TABLES = [
    "nation", # put first (reference tables)
    "part", "supplier",
    "partsupp", # depends on part and supplier
    "orders", # depends on customer (tapi di query 9, customer ga dipake)
    "lineitem", # last to be loaded, since it depends on orders and partsupp
]

#  Directory of table
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(SCRIPT_DIR, "..", "..", "dataset")
# number of rows per chunk
CHUNKSIZE=100000

STAGING_AREA = {
    "df_nation": None,
    "df_part": None,
    "df_supplier": None,
    "df_partsupp": None,
    "df_orders": None
}
# kalo semua kecuali lineitem di sini, bakal ada sekitar 14550 MB di memori, jadi usahain jangan wkwkwk


# EXTRACTION
def extract_data(table_name, file_format, chunk_size=CHUNKSIZE):
    """
    Extract data from file in chunks
    :param table_name: Table name (iterative) e.g. 'region', 'nation', etc.
    :param file_format: File format e.g. 'csv', 'jsonl', 'parquet'
    :param chunk_size: Number of rows per chunk (set to CHUNKSIZE by default)
    """
    # defining file path
    file_path = os.path.join(DATASET_DIR, f"{file_format}", f"{table_name}.{file_format}")

    # if data format == 'csv'
    if file_format == 'csv':
        df = pd.read_csv(file_path, chunksize=chunk_size)
        for chunk in df:
            yield chunk
    # if data format == 'jsonl'
    elif file_format == 'jsonl':
        df = pd.read_json(file_path,lines=True, chunksize=chunk_size)
        for chunk in df:
            yield chunk
    # if data format == 'parquet'
    elif file_format == 'parquet':
        pq_file = pq.ParquetFile(file_path)
        # doing this for chunking purposes
        for chunk in pq_file.iter_batches():
            df = chunk.to_pandas()
            yield df

# TRANSFORMATION
def transform_data(df, table_name, staging_area=STAGING_AREA):
    """
    Transform data
    TO-DO:
    1. do a .str.strip() for string columns
    2. change datatype, e.g. from string to date (for dates)
    3. do the QUERY 9 transformation
    """

    # 1. stripping whitespaces for string columns
    str_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # 2. change datatype for date columns
    date_cols = [col for col in df.columns if ('date' in col.lower())]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 3. QUERY 9 transformation
    # -- filters (can reduce rows immediately)
    if table_name == "part":
        if staging_area["df_part"] is None:
            staging_area["df_part"] = df[df['p_name'].str.contains('green', case=False, na=False)]
        else:
            staging_area["df_part"] = pd.concat([staging_area["df_part"], df[df['p_name'].str.contains('green', case=False, na=False)]], ignore_index=True)
    
    # -- table joins (tabel yang kecil)
        # 'Nation'
    elif table_name == "nation":
        if staging_area["df_nation"] is None:
            staging_area["df_nation"] = df
        else:
            staging_area["df_nation"] = pd.concat([staging_area["df_nation"], df], ignore_index=True)
        # 'Supplier'
    elif table_name == "supplier":
        if staging_area["df_supplier"] is None:
            staging_area["df_supplier"] = df.merge(staging_area["df_nation"], how='inner', left_on='s_nationkey', right_on='n_nationkey')
        else:
            staging_area["df_supplier"] = pd.concat([staging_area["df_supplier"], df.merge(staging_area["df_nation"], how='inner', left_on='s_nationkey', right_on='n_nationkey')], ignore_index=True)
    
        # 'Partsupp', tapi cuma suppkey, supplycost, dan partkey
    elif table_name == "partsupp":
        if staging_area["df_partsupp"] is None:
            staging_area["df_partsupp"] = df[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]
        else:
            staging_area["df_partsupp"] = pd.concat([staging_area["df_partsupp"], df[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]], ignore_index=True)
        # 'Orders', tapi cuma orderkey dan orderdate
    elif table_name == "orders":
        if staging_area["df_orders"] is None:
            staging_area["df_orders"] = df[['o_orderkey', 'o_orderdate']]
        else:
            staging_area["df_orders"] = pd.concat([staging_area["df_orders"], df[['o_orderkey', 'o_orderdate']]], ignore_index=True)

    # -- table joins (tabel besar)
    elif table_name == "lineitem":
        df = df.merge(staging_area["df_part"], how='inner', left_on='l_partkey', right_on='p_partkey')
        df = df.merge(staging_area["df_supplier"], how='inner', left_on='l_suppkey', right_on='s_suppkey')
        # nation sudah di-join di supplier, di tahap sebelumnya.
        df = df.merge(staging_area["df_partsupp"], how='inner', left_on=['l_partkey', 'l_suppkey'], right_on=['ps_partkey', 'ps_suppkey'])
        df = df.merge(staging_area["df_orders"], how='inner', left_on='l_orderkey', right_on='o_orderkey')

        # extract year from orders
        df['o_year'] = df['o_orderdate'].dt.year

        # amount = (price * (1-discount)) - (cost * quantity)
        df['amount'] = (df['l_extendedprice'] * (1 - df['l_discount'])) - (df['ps_supplycost'] * df['l_quantity'])
        df = df.groupby(["n_name", "o_year"], as_index=False).agg({"amount": "sum"})

        # rename columns, to match the Query 9 standards
        df = df.rename(columns={
            "n_name": "nation",
            "amount": "sum_profit"
        })

        return df[['nation', 'o_year', 'sum_profit']].sort_values(by=['nation', 'o_year'], ascending=[True, False])

def load_data(df, table_name):
    # data loading nanti akan dilakukan di function yang ini
    if table_name == "lineitem":
        try:
            print(f"Loading {len(df)} rows into etl_query9 table...")
            df.to_sql("etl_query9", con=engine, if_exists='append', index=False)
        except Exception as e:
            print(f"Error loading data: {e}")

# MAIN FUNCTION
if __name__ == "__main__":
    print(f"Starting ETL process at {datetime.now()}")
    for table in TABLES:
        data_stream = extract_data(table, 'csv')
        print(f"Extracted '{table}' table.")
        for chunk in data_stream:
            transformed_data = transform_data(chunk, table)
        if transformed_data is not None:
            load_data(transformed_data, table)
            print(f"Loaded {table} table.")
    print(f"ETL process completed at {datetime.now()}")