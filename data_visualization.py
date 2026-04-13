from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder.appName("Visualization Dashboard").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# LOAD DATA (IMPORTANT FIX)
# =========================
df_final = spark.read.parquet("data/processed/reviews/")

# =========================
# TO PANDAS
# =========================
pdf = df_final.select("overall", "sentiment", "review_length", "asin").toPandas()

# =========================
# DASHBOARD STYLE
# =========================
sns.set_theme(style="whitegrid")

fig, axes = plt.subplots(2, 2, figsize=(15, 10))

fig.suptitle("Kindle Reviews Dashboard", fontsize=18, fontweight="bold")

# =========================
# 1. RATINGS DISTRIBUTION
# =========================
pdf["overall"].value_counts().sort_index().plot(kind="bar", ax=axes[0, 0])
axes[0, 0].set_title("Ratings Distribution")

# =========================
# 2. SENTIMENT
# =========================
pdf["sentiment"].value_counts().plot(kind="pie", autopct="%1.1f%%", ax=axes[0, 1])
axes[0, 1].set_title("Sentiment Distribution")
axes[0, 1].set_ylabel("")

# =========================
# 3. LENGTH vs RATING
# =========================
sns.scatterplot(data=pdf.sample(5000), x="review_length", y="overall", ax=axes[1, 0])
axes[1, 0].set_title("Review Length vs Rating")

# =========================
# 4. TOP BOOKS
# =========================
top_asins = pdf["asin"].value_counts().head(10)

top_asins.plot(kind="bar", ax=axes[1, 1])
axes[1, 1].set_title("Top Books")

plt.tight_layout()
plt.savefig("dashboard.png", dpi=300)
plt.show()
