import os
import time # untuk time.sleep (untuk review Spark Web UI agar ada waktu)
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# FILE PATH
SCRIPT_DIR_PATH = SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(SCRIPT_DIR, "..", "..", "dataset")
SPARK_LOG_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "logs", "spark-logs"))


# SPARK HISTORY LOGS NAMING
ARCH = "HYBRID"
FILE_FORMAT_RUN = os.getenv("FILE_FORMAT_RUN", "csv")
RUN_ID = os.getenv("RUN_ID", "run1")
SCALE_FACTOR = os.getenv("SCALE_FACTOR", "sf5")

# SPARK BUILDER
spark = (
    SparkSession.builder
        .appName("TPCH_Query9_{ARCH}_{FILE_FORMAT_RUN}_{RUN_ID}_{SCALE_FACTOR}")
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4")

        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file:///{SPARK_LOG_DIR.replace(os.sep, '/')}")
        .getOrCreate()
)

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
FILE_FORMAT = "csv" # csv, jsonl, parquet

# EXTRACTION PHASE
def extract_data(table_name, file_format):
    spark.sparkContext.setJobGroup("Hybrid_Extract", f"Hybrid Extract Phase: {table_name}")
    spark.sparkContext.setJobDescription(f"[Hybrid] Extract '{table_name}' from {file_format}")
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
    spark.sparkContext.setJobGroup("Hybrid_Clean", f"Hybrid Clean Phase: {table_name}")
    spark.sparkContext.setJobDescription(f"[Hybrid] Clean '{table_name}' - select & cast columns")
    print(f"Starting to clean table '{table_name}'...")
    if table_name == "lineitem":
        df = df.select(
            col("l_orderkey").cast("integer"),
            col("l_partkey").cast("integer"),
            col("l_suppkey").cast("integer"),
            col("l_quantity").cast("double"),
            col("l_extendedprice").cast("double"),
            col("l_discount").cast("double")
        )
    
    elif table_name == "orders":
        df = df.select(
            col("o_orderkey").cast("integer"),
            col("o_orderdate").cast("date")
        )

    elif table_name == "part":
        df = df.select(
            col("p_partkey").cast("integer"),
            col("p_name").cast("string")
        )

    elif table_name == "supplier":
        df = df.select(
            col("s_suppkey").cast("integer"),
            col("s_nationkey").cast("integer")
        )
    elif table_name == "nation":
        df = df.select(
            col("n_nationkey").cast("integer"),
            col("n_name").cast("string")
        )
    elif table_name == "partsupp":
        df = df.select(
            col("ps_partkey").cast("integer"),
            col("ps_suppkey").cast("integer"),
            col("ps_supplycost").cast("double")
        )

    print(f"Table '{table_name}' cleaned successfully.")
    return df

def load_data(table_name, df):
    spark.sparkContext.setJobGroup("Hybrid_Load", f"Hybrid Load Phase: {table_name}")
    spark.sparkContext.setJobDescription(f"[Hybrid] Load '{table_name}' into raw.hybrid_{table_name}")
    print(f"Starting to load table '{table_name}'...")
    try:
        df.write.jdbc(
            url=DB_URL,
            table=f"raw.hybrid_{table_name}",
            mode="append",
            properties=DB_PROPERTIES
        )
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    spark.sparkContext.setJobGroup("Hybrid_Pipeline", "TPC-H Query 9 - Hybrid Pipeline")
    for table in TABLE_NAME:
        print(f"=== Starting ECL Process for table '{table}' ===")
        extracted_data = extract_data(table, FILE_FORMAT)
        df_cleaned = clean_data(table, extracted_data)
        load_data(table, df_cleaned)
        print(f"Table '{table}' loaded successfully.\n")
    spark.sparkContext.setJobDescription("[Hybrid] Pipeline Complete")
    
    KEEP_UI_OPEN = os.getenv("KEEP_UI_OPEN", "false").lower() in ("true", "1", "yes")

    print("Hybrid Extract-Clean-Load process done.")

    if KEEP_UI_OPEN:
        print("Spark UI is available at http://localhost:4040")
        input("Press Enter to stop SparkSession...")

    spark.stop()
    print("SparkSession stopped.")