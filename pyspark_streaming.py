from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder.appName("Streaming Simulation Kindle").getOrCreate()

# =========================
# LOAD FULL DATA
# =========================
df = spark.read.json("data/raw/Kindle_Store_5/*.json")

df = df.select("asin", "overall", "reviewText")

# =========================
# SIMULATE STREAMING (MICRO-BATCH)
# =========================
batch_size = 50000  # puedes cambiarlo

total = df.count()
num_batches = total // batch_size + 1

results = []

for i in range(num_batches):

    batch = df.limit(batch_size).offset(i * batch_size)

    if batch.count() == 0:
        continue

    processed = batch.groupBy("asin").agg(
        F.count("*").alias("batch_reviews"), F.avg("overall").alias("batch_avg_rating")
    )

    print(f"Processed batch {i+1}/{num_batches}")
    processed.show(5)

    results.append(processed)

# =========================
# COMBINE RESULTS
# =========================
final_stream = results[0]
for r in results[1:]:
    final_stream = final_stream.union(r)

final_stream.groupBy("asin").agg(F.avg("batch_avg_rating").alias("final_avg")).show(10)
