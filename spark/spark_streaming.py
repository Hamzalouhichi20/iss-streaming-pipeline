from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DoubleType, StringType, LongType

schema = StructType() \
    .add("timestamp", LongType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("altitude", DoubleType()) \
    .add("velocity", DoubleType()) \
    .add("country", StringType())

spark = SparkSession.builder \
    .appName("KafkaSparkISS") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/gcs-key.json") \
    .getOrCreate()

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iss_data") \
    .option("startingOffsets", "latest") \
    .load()

df_values = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

df_values.writeStream \
    .format("parquet") \
    .option("path", "gs://iss-data-lake/iss/") \
    .option("checkpointLocation", "gs://iss-data-lake/checkpoints/iss/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

