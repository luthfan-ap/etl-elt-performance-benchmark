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
    "nation", "region", # put first (reference tables)
    "customer", # depends on nation
    "part", "supplier",
    "partsupp", # depends on part and supplier
    "orders", # depends on customer (tapi di query 9, customer ga dipake)
    "lineitem" # last to be loaded, since it depends on orders and partsupp
]

#  Directory of table
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(SCRIPT_DIR, "..", "..", "dataset")
# number of rows per chunk
CHUNKSIZE=100000

def extract_load(table_name, file_format):
    """
    Extract data from file and load to database
    """
    file_path = os.path.join(DATASET_DIR, f"{file_format}", f"{table_name}.{file_format}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found")
    
    reader = None

    if file_format == "csv":
        reader = pd.read_csv(file_path, chunksize=CHUNKSIZE)
    elif file_format == "jsonl":
        reader = pd.read_json(file_path, lines=True, chunksize=CHUNKSIZE)
    elif file_format == "parquet":
        pq_file = pq.ParquetFile(file_path)
        reader = (batch.to_pandas() for batch in pq_file.iter_batches(batch_size=CHUNKSIZE))

    else:
        print(f"Unsupported file format: {file_format}")
        return

    for i, chunk in enumerate(reader):
        # cleaning
        str_cols = chunk.select_dtypes(include=['object', 'string']).columns
        for col in str_cols:
            chunk[col] = chunk[col].str.strip()

        # loading
        try:
            chunk.to_sql(
                table_name,
                con=engine,
                schema='raw_layer',
                if_exists='append',
                index=False
            )
        except Exception as e:
            print(f"Error loading chunk {i}: {e}")

if __name__ == "__main__":
    print(f"Starting ELT process at {datetime.now()}")
    for table in TABLES:
        print(f"Extracting {table} table...")
        extract_load(table, file_format="csv")
        print(f"Extracted table {table}")
    print(f"ELT process completed at {datetime.now()}")