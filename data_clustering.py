from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder.appName("AI Component - Clustering").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# LOAD DATA
# =========================
df = spark.read.parquet("data/processed/reviews/")

# =========================
# FEATURE SELECTION
# =========================
df_ml = df.select("overall", "review_length").na.drop()

# =========================
# VECTOR ASSEMBLER
# =========================
assembler = VectorAssembler(
    inputCols=["overall", "review_length"], outputCol="features_raw"
)

df_vec = assembler.transform(df_ml)

# =========================
# SCALING
# =========================
scaler = StandardScaler(inputCol="features_raw", outputCol="features")

scaler_model = scaler.fit(df_vec)
df_scaled = scaler_model.transform(df_vec)

# =========================
# KMEANS MODEL
# =========================
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="cluster")

model = kmeans.fit(df_scaled)

df_clustered = model.transform(df_scaled)

# =========================
# CLUSTER INSPECTION
# =========================
df_clustered.groupBy("cluster").agg(
    F.avg("overall").alias("avg_rating"),
    F.avg("review_length").alias("avg_length"),
    F.count("*").alias("count"),
).show()

# =========================
# SAVE RESULTS
# =========================
df_clustered.write.mode("overwrite").parquet("data/ai/clusters/")

print("✔ AI CLUSTERING MODEL COMPLETED")
