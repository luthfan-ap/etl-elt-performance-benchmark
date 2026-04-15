import os
import time # untuk time.sleep (untuk review Spark Web UI agar ada waktu)
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# SPARKSESSION BUILDER
spark = (
    SparkSession.builder
        .appName("TPCH_Query9_Hybrid")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")
        .getOrCreate()
)

# FILE PATH
SCRIPT_DIR_PATH = SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(SCRIPT_DIR, "..", "..", "dataset")

# DB CONNECTION PROPERTIES
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

# NAMES (untuk ECLT, tabel yang di-load adalah tabel yang relevan saja)
TABLE_NAME = [
    "nation", # tabel referensi, taruh pertama
    "part", "supplier",
    "partsupp", # depends ke part and supplier
    "orders", # depends ke customer (tapi di query 9, customer ga dipake)
    "lineitem", # terakhir, karena depends ke orders and partsupp
]
FILE_FORMAT = [
    "csv", "json", "parquet"
]

# EXTRACTION PHASE
def extract_data(table_name, file_format):
    print(f"Starting to extract table '{table_name}'...")
    if file_format == "csv": # butuh config header=True khusus untuk CSV.
        df = spark.read \
            .format(file_format) \
            .option("header", "True") \
            .load(f"{DATASET_DIR}/{file_format}/{table_name}.{file_format}")
    elif file_format == "jsonl": # format "jsonl" dihandle oleh .format("json") di pyspark, jadi butuh special handling
        df = spark.read \
            .format("json") \
            .load(f"{DATASET_DIR}/{file_format}/{table_name}.{file_format}")
    elif file_format == "parquet": # normal case handling
        df = spark.read \
            .format(file_format) \
            .load(f"{DATASET_DIR}/{file_format}/{table_name}.{file_format}")
    else:
        raise ValueError(f"Format {file_format} tidak sesuai dengan pipeline ini.")
    print(f"Table '{table_name}' extracted successfully.")
    return df

# CLEANING PHASE
def clean_data(table_name, df):
    print(f"Starting to clean table '{table_name}'...")
    if table_name == "lineitem":
        df = df.select(
            col("l_orderkey"),
            col("l_partkey"),
            col("l_suppkey"),
            col("l_quantity"),
            col("l_extendedprice"),
            col("l_discount")
        )
    
    elif table_name == "orders":
        df = df.select(
            col("o_orderkey"),
            col("o_orderdate")
        )

    elif table_name == "part":
        df = df.select(
            col("p_partkey"),
            col("p_name")
        )

    elif table_name == "supplier":
        df = df.select("s_suppkey", "s_nationkey")
    elif table_name == "nation":
        df = df.select("n_nationkey", "n_name")
    elif table_name == "partsupp":
        df = df.select(col("ps_partkey"), col("ps_suppkey"), col("ps_supplycost").cast("double"))

    print(f"Table '{table_name}' cleaned successfully.")
    return df

def load_data(table_name, df):
    print(f"Starting to load table '{table_name}'...")
    try:
        df.write.jdbc(
            url=DB_URL,
            table=f"raw.hybrid_{table_name}",
            mode="overwrite",
            properties=DB_PROPERTIES
        )
    except Exception as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    for table in TABLE_NAME:
        print(f"=== Starting ECL Process for table '{table}' ===")
        extracted_data = extract_data(table, "csv")
        df_cleaned = clean_data(table, extracted_data)
        load_data(table, df_cleaned)
        print(f"Table '{table}' loaded successfully.\n")
    
    print("E, C, L Process done. You can access the http://localhost:4040 for detailed monitoring.")
    print("Press CTRL+C to terminate the process.")

    time.sleep(3600)
    spark.stop()