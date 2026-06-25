import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
import time # untuk time.sleep (untuk review Spark Web UI agar ada waktu)
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# FILE PATH
SCRIPT_DIR_PATH = SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(SCRIPT_DIR, "..", "..", "dataset")
SPARK_LOG_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", "logs", "spark-logs"))

# SPARK BUILDER
spark = (
    SparkSession.builder
        .appName("TPCH_Query9_ELT")
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
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
DB_RAW_SCHEMA = os.getenv("DB_RAW_SCHEMA")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASS,
    "driver": "org.postgresql.Driver"
}

# NAMES (untuk ELT, beberapa tabel yang relevan saja yang di-load ke data warehouse target)
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
    spark.sparkContext.setJobGroup("ELT_Extract", f"ELT Extract Phase: {table_name}")
    spark.sparkContext.setJobDescription(f"[ELT] Extract '{table_name}' from {file_format}")

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
    spark.sparkContext.setJobGroup("ELT_Load", f"ELT Load Phase: {table_name}")
    spark.sparkContext.setJobDescription(f"[ELT] Load '{table_name}' into raw.elt_{table_name}")
    print(f"Starting to Load table '{table_name}' into {DB_RAW_SCHEMA}.elt_{table_name}...")
    try:
        df.write.jdbc(
            url=DB_URL,
            table=f"{DB_RAW_SCHEMA}.elt_{table_name}",
            mode="append",
            properties=DB_PROPERTIES
        )
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

if __name__ == "__main__":
    spark.sparkContext.setJobGroup("ELT_Pipeline", "TPC-H Query 9 - ELT Pipeline")
    for table in TABLE_NAME:
        print(f"=== Starting EL process for table '{table}'... ===")
        extracted_data = extract_data(table, FILE_FORMAT)
        load_data(table, extracted_data)
        print(f"Table '{table}' loaded successfully.\n")
    spark.sparkContext.setJobDescription("[ELT] Pipeline Complete")
    
    print("E and L Process done. You can access the http://localhost:4040 for detailed monitoring.")
    print("Press CTRL+C to terminate the process.")
    time.sleep(3600)
    spark.stop()