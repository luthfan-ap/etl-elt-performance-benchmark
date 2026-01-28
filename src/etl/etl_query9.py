# Import dependencies
import os
import pandas as pd
import pyarrow.parquet as pq

# List of table names
TABLES = [
    "region", "nation", # put first (reference tables)
    "part", "supplier",
    "partsupp", # depends on part and supplier
    "customer", # depends on nation
    "orders", # depends on customer
    "lineitem", # last to be loaded, since it depends on orders and partsupp
]

#  Directory of table
DATASET_DIR = "../../dataset/"
# number of rows per chunk
CHUNKSIZE=100000


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
# def transform_data():
    """
    Transform data
    TO-DO:
    1. do a .str.strip() for string columns
    2. change datatype, e.g. from string to date (for dates)
    3. recheck on the int and float datatypes
    4. do the QUERY 9 transformation
    """


if __name__ == "__main__":
    # Example usage
    for table in TABLES:
        print(f"Extracting data for table: {table}")
        for chunk in extract_data(table, 'csv'):
            print(f"Extracted chunk with shape: {chunk.shape}")

            if table == "orders":
                print(f"Data types for {table}")
                print(chunk.dtypes)
                print()