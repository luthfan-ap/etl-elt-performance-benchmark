import os
import time # untuk time.sleep (untuk review Spark Web UI agar ada waktu)
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# SPARK BUILDER
spark = (
    SparkSession.builder
        .appName("TPCH_Query9_ETL")
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

# NAMES (untuk ETL, beberapa tabel yang relevan saja yang di-load ke data warehouse target)
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
    print(f"Extracting table '{table_name}.{file_format}'...")

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
    return df

# TRANSFORMATION PHASE
def transform_data(df):
    print("\n=== TRANSFORMATION ===")
    print("Starting the transformation phase...")

    df_nation   = df["nation"]
    df_part     = df["part"]
    df_supplier = df["supplier"]
    df_partsupp = df["partsupp"]
    df_orders   = df["orders"]
    df_lineitem = df["lineitem"]

    # FILTER: hanya yang contains 'green' di p_name
    df_part_filtered = df_part.filter(F.lower(F.col("p_name")).contains("green"))

    # JOIN: supplier dan nation (s_nationkey = n_nationkey)
    df_supplier_nation = df_supplier.join(
        df_nation,
        df_supplier["s_nationkey"] == df_nation["n_nationkey"],
        "inner"
    )

    # JOIN: lineitem dan filtered part table (l_partkey = p_partkey)
    df_joined = df_lineitem.join(
        df_part_filtered,
        df_lineitem["l_partkey"] == df_part_filtered["p_partkey"],
        "inner"
    )

    # JOIN: df dan supplier+nation (l_suppkey = s_suppkey)
    df_joined = df_joined.join(
        df_supplier_nation,
        df_joined["l_suppkey"] == df_supplier_nation["s_suppkey"],
        "inner"
    )

    # JOIN: df dan partsupp (l_suppkey = ps_suppkey AND l_partkey = ps_partkey)
    df_joined = df_joined.join(
        df_partsupp,
        (df_joined["l_suppkey"] == df_partsupp["ps_suppkey"]) &
        (df_joined["l_partkey"] == df_partsupp["ps_partkey"]),
        "inner"
    )

    # JOIN: df dan orders (l_orderkey = o_orderkey)
    df_joined = df_joined.join(
        df_orders,
        df_joined["l_orderkey"] == df_orders["o_orderkey"],
        "inner"
    )

    # Calculate o_year dan amount
    df_profit = df_joined.select(
        F.col("n_name").alias("nation"),
        F.year(F.col("o_orderdate").cast("date")).alias("o_year"),
        (
            (F.col("l_extendedprice").cast("double") * (1 - F.col("l_discount").cast("double")))
            - (F.col("ps_supplycost").cast("double") * F.col("l_quantity").cast("double"))
        ).alias("amount")
    )

    # GROUP BY: nation, o_year dan hitung sum(amount) as sum_profit
    df_result = df_profit.groupBy("nation", "o_year") \
        .agg(F.sum("amount").alias("sum_profit"))

    # ORDER BY: nation ASC, o_year DESC
    df_result = df_result.orderBy(
        F.col("nation").asc(),
        F.col("o_year").desc()
    )

    return df_result

# LOADING PHASE
def load_data(df):
    print("\n=== LOADING ===")
    print("Starting the loading phase...\nLoading into 'result_etl.etl_query9' table...")
    try:
        df.write \
            .jdbc(
                url=DB_URL,
                table="result_etl.etl_query9",
                mode="overwrite",
                properties=DB_PROPERTIES
            )
        print("Table 'result_etl.etl_query9' loaded successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")


if __name__ == "__main__":
    extracted_dfs = {} # wadah sementara untuk semua tabel yang udah di extract
    print("\n=== EXTRACTION ===")
    for table in TABLE_NAME:
        extracted_dfs[table] = extract_data(table, "csv")
    transformed_data = transform_data(extracted_dfs)
    load_data(transformed_data)

    print("ETL Process done. You can access the http://localhost:4040 for detailed monitoring.")
    print("Press CTRL+C to terminate the process.")
    time.sleep(3600)
    spark.stop()