import os
import time # untuk time.sleep (untuk review Spark Web UI agar ada waktu)
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# SPARK BUILDER
spark = (
    SparkSession.builder
        .appName("TPCH_Query9_ELT")
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
DB_RAW_SCHEMA = os.getenv("DB_RAW_SCHEMA")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

# NAMES (untuk ELT, semua tabel di-load ke data warehouse target)
TABLE_NAME = [
    "nation", "region", # tabel referensi, taruh pertama
    "customer",
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

    print(f"Starting to Extract table '{table_name}'...")

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

# LOADING PHASE
def load_data(table_name, df):
    print(f"Starting to Load table '{table_name}'...")
    try:
        df.write.jdbc(
            url=DB_URL,
            table=f"raw.elt_{table_name}",
            mode="overwrite",
            properties=DB_PROPERTIES
        )
    except Exception as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    for table in TABLE_NAME:
        print(f"=== Starting EL process for table '{table}'... ===")
        extracted_data = extract_data(table, "csv")
        load_data(table, extracted_data)
        print(f"Table '{table}' loaded successfully.\n")
    
    print("E and L Process done. You can access the http://localhost:4040 for detailed monitoring.")
    print("Press CTRL+C to terminate the process.")
    time.sleep(3600)
    spark.stop()