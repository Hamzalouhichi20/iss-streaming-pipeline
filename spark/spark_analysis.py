from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg, count
spark = SparkSession.builder \
    .appName("ISSDataAnalysis") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/gcs-key.json") \
    .getOrCreate()

# Lecture des fichiers Parquet depuis GCS
df = spark.read.parquet("gs://iss-data-lake/iss/")

# Statistiques globales
df.select("altitude", "velocity").describe().show()
df = df.withColumn("country", when(col("country") == "Unknown", "Ocean").otherwise(col("country")))

# Moyenne par pays
df.groupBy("country").agg(
    avg("altitude").alias("avg_altitude"),
    avg("velocity").alias("avg_velocity"),
    count("*").alias("count")
).orderBy("count", ascending=False).show()

spark.stop()
