from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Amazon_Kindle_BigData")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)
