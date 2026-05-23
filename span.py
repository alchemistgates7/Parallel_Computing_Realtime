from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ==============================
# Kafka Configuration
# ==============================
KAFKA_BOOTSTRAP_SERVER = "mykafka:9092"
TOPIC_NAME = "bank-transactions"


# ==============================
# MySQL Configuration
# ==============================
MYSQL_URL = "jdbc:mysql://MS3_Mysql:3306/MS3"
MYSQL_TABLE = "suspicious_transactions"
MYSQL_USER = "root"
MYSQL_PASSWORD = "secret"


# ==============================
# Create Spark Session
# ==============================
spark = SparkSession.builder \
    .appName("BankTransactionFraudDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==============================
# Define Kafka Message Schema
# ==============================
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# ==============================
# Read Stream from Kafka
# ==============================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()


# ==============================
# Convert Kafka JSON Message to Columns
# ==============================
transactions = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")


# ==============================
# Analyze Transactions
# Rule:
# Suspicious if amount > 5000
# OR if it is a TRANSFER outside Germany
# ==============================
analyzed = transactions.withColumn(
    "status",
    when(
        (col("amount") > 5000) |
        (
            (col("transaction_type") == "TRANSFER") &
            (col("country") != "Germany")
        ),
        "SUSPICIOUS"
    ).otherwise("NORMAL")
)


# ==============================
# Keep Only Suspicious Transactions
# ==============================
suspicious = analyzed.filter(col("status") == "SUSPICIOUS")


# ==============================
# Write Each Batch to MySQL
# Important:
# We remove "timestamp" because the MySQL table does not have this column.
# MySQL will automatically fill "created_at".
# ==============================
def write_to_mysql(batch_df, batch_id):
    clean_df = batch_df.select(
        "transaction_id",
        "account_id",
        "amount",
        "transaction_type",
        "country",
        "status"
    )

    clean_df.write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .mode("append") \
        .save()


# ==============================
# Start Streaming Query
# ==============================
query = suspicious.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-bank-checkpoint-v2") \
    .start()


query.awaitTermination()
