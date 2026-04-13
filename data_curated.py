from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc, col

spark = (
    SparkSession.builder.appName("CURATED_LAYER")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# -----------------------------
# LEER PROCESSED
# -----------------------------
df = spark.read.parquet("data/processed/")

# =====================================================
# 📚 1. TOP LIBROS (rating + popularidad)
# =====================================================
top_books = (
    df.groupBy("asin")
    .agg(avg("overall").alias("avg_rating"), count("*").alias("num_reviews"))
    .filter(col("num_reviews") >= 20)
    .orderBy(desc("avg_rating"))
)

top_books.write.mode("overwrite").parquet("data/curated/top_books/")

# =====================================================
# 👤 2. USUARIOS MÁS ACTIVOS
# =====================================================
top_users = (
    df.groupBy("reviewerID")
    .agg(count("*").alias("num_reviews"), avg("overall").alias("avg_rating_given"))
    .orderBy(desc("num_reviews"))
)

top_users.write.mode("overwrite").parquet("data/curated/top_users/")

# =====================================================
# ⭐ 3. DISTRIBUCIÓN DE RATINGS
# =====================================================
rating_dist = df.groupBy("overall").count().orderBy("overall")

rating_dist.write.mode("overwrite").parquet("data/curated/rating_distribution/")

print("CURATED LAYER OK ✔️")
