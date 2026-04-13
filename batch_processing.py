from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt

# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder.appName("Kindle Batch Processing Fixed")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.files.maxPartitionBytes", "64m")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# =========================
# SCHEMA
# =========================
schema = StructType(
    [
        StructField("overall", DoubleType(), True),
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("reviewText", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("unixReviewTime", LongType(), True),
    ]
)

# =========================
# LOAD DATA
# =========================
path = "data/raw/Kindle_Store_5/*.json"
df = spark.read.schema(schema).json(path)

print("RAW ROWS:", df.count())
df.printSchema()

# =========================
# VALIDATE DISTRIBUTION (IMPORTANT)
# =========================
df.groupBy("overall").count().orderBy("overall").show()

# =========================
# CLEANING
# =========================
df_clean = df.filter(
    (F.col("asin").isNotNull())
    & (F.col("overall").isNotNull())
    & (F.col("overall").between(1, 5))
)

print("CLEAN ROWS:", df_clean.count())

# =========================
# FEATURE ENGINEERING
# =========================
df_features = df_clean.withColumn(
    "review_length", F.length(F.col("reviewText"))
).withColumn(
    "sentiment",
    F.when(F.col("overall") >= 4, "positive")
    .when(F.col("overall") == 3, "neutral")
    .otherwise("negative"),
)

# =========================
# BOOK STATS (IMPORTANT FIX)
# =========================
book_stats = (
    df_features.groupBy("asin")
    .agg(
        F.count("*").alias("num_reviews"),
        F.avg("overall").alias("avg_rating"),
        F.stddev("overall").alias("rating_std"),
    )
    .filter(F.col("num_reviews") >= 1000)  # 🔥 evita falsos 5 estrellas
)

# =========================
# TOP BOOKS (REALISTIC)
# =========================
top_books = book_stats.orderBy(F.desc("avg_rating"))

print("TOP BOOKS SAMPLE:")
top_books.show(10)

# =========================
# SAVE LAYERS
# =========================
df_features.write.mode("overwrite").parquet("data/processed/reviews/")
book_stats.write.mode("overwrite").parquet("data/curated/book_stats/")
top_books.limit(20).write.mode("overwrite").csv("data/curated/top_books/", header=True)

# =========================
# VISUALIZATION (FIXED)
# =========================
pdf = top_books.limit(10).toPandas()

plt.figure(figsize=(10, 5))
plt.bar(pdf["asin"].astype(str), pdf["avg_rating"])

plt.xticks(rotation=45)
plt.title("Top Books (Filtered by >=1000 reviews)")
plt.xlabel("ASIN")
plt.ylabel("Average Rating")

plt.tight_layout()
plt.savefig("top_books.png")
plt.show()
