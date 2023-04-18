from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType, IntegerType, StringType,FloatType,DateType

spark = SparkSession.builder \
    .appName("Cassandra to Spark DataFrame") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

# Cassandra tablosunun ismi
table_name = "log_data"

# Cassandra anahtar uzayının ismi
keyspace = "cassandra_tutorial"

# StructType oluşturma
log_data_schema = StructType([
        StructField("interval", DoubleType(), True),
        StructField("date_time_", DateType(), True),
        StructField("ip", StringType(), True),
        StructField("url", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("size", IntegerType(), True),
        StructField("duration", DoubleType(), True),
])
# Kafka verilerini okuma ve DataFrame oluşturma
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "34.125.86.34:9092") \
  .option("subscribe", "ornek") \
  .load()

# JSON verileri StructType'a dönüştürme
df = df.select(from_json(df["value"].cast("string"), log_data_schema).alias("activation")) 

df = df.select("activation.interval","activation.date_time_", "activation.ip", "activation.url", "activation.status",
                "activation.size", "activation.duration"
                )# Cassandra tablosuna yazma

df = df.filter(df["status"] == 200)

# df.writeStream.format("console").outputMode("append").start()


df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("keyspace", keyspace) \
    .option("table", table_name) \
    .start()

df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://pyspark-m:8020/user/denemegcp/") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
