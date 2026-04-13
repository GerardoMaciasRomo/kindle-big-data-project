from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("PROCESSED_LAYER")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# -----------------------------
# LEER RAW PARQUET
# -----------------------------
df = spark.read.parquet("data/raw/raw_parquet/")

# -----------------------------
# LIMPIEZA SEGURA
# -----------------------------
df_clean = df.select(
    col("asin"),
    col("reviewerID"),
    col("overall").cast("double"),
    col("reviewText"),
    col("unixReviewTime").cast("long"),
)

# -----------------------------
# FILTRAR NULOS
# -----------------------------
df_clean = df_clean.filter(
    col("asin").isNotNull() & col("reviewerID").isNotNull() & col("overall").isNotNull()
)

# -----------------------------
# GUARDAR PROCESSED
# -----------------------------
df_clean.write.mode("overwrite").parquet("data/processed/")

print("PROCESSED LAYER OK ✔️")
