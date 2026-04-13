from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("RAW_LAYER")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

# -----------------------------
# LEER JSONL (Amazon Kindle)
# -----------------------------
df = (
    spark.read.option("multiLine", False)
    .option("mode", "DROPMALFORMED")
    .json("data/raw/Kindle_Store_5/")
)

# -----------------------------
# DEBUG (opcional)
# -----------------------------
df.printSchema()
print("PARTITIONS:", df.rdd.getNumPartitions())

# -----------------------------
# GUARDAR RAW EN PARQUET
# -----------------------------
df.write.mode("overwrite").option("maxRecordsPerFile", 1000000).parquet(
    "data/raw/raw_parquet/"
)

print("RAW LAYER OK ✔️")
