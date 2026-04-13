from pyspark.sql import SparkSession, functions as F
import sqlite3

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder.appName("Analytical Data Model").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# LOAD PROCESSED DATA
# =========================
df = spark.read.parquet("data/processed/reviews/")

# =========================
# FACT TABLE (GRANULAR DATA)
# =========================
fact_reviews = df.select(
    "asin", "reviewerID", "overall", "review_length", "sentiment", "unixReviewTime"
)

# =========================
# DIMENSION TABLE 1: BOOK STATS
# =========================
dim_books = df.groupBy("asin").agg(
    F.count("*").alias("total_reviews"),
    F.avg("overall").alias("avg_rating"),
    F.stddev("overall").alias("rating_std"),
    F.max("overall").alias("max_rating"),
    F.min("overall").alias("min_rating"),
)

# =========================
# DIMENSION TABLE 2: SENTIMENT
# =========================
dim_sentiment = df.groupBy("sentiment").agg(F.count("*").alias("total"))

# =========================
# DIMENSION TABLE 3: USERS
# =========================
dim_users = df.groupBy("reviewerID").agg(
    F.count("*").alias("reviews_count"), F.avg("overall").alias("avg_user_rating")
)

# =========================
# CONVERT TO PANDAS (FOR DB)
# =========================
fact_pd = fact_reviews.toPandas()
books_pd = dim_books.toPandas()
sent_pd = dim_sentiment.toPandas()
users_pd = dim_users.toPandas()

# =========================
# STORE IN SQLITE DATABASE
# =========================
conn = sqlite3.connect("analytics.db")

fact_pd.to_sql("fact_reviews", conn, if_exists="replace", index=False)
books_pd.to_sql("dim_books", conn, if_exists="replace", index=False)
sent_pd.to_sql("dim_sentiment", conn, if_exists="replace", index=False)
users_pd.to_sql("dim_users", conn, if_exists="replace", index=False)

conn.close()

print("✔ ANALYTICAL DATA MODEL CREATED AND STORED IN DATABASE")
