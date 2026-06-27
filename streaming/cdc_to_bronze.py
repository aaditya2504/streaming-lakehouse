from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
KAFKA   = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
AWS     = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
ICEAWS  = "org.apache.iceberg:iceberg-aws-bundle:1.6.1"

spark = (
    SparkSession.builder.appName("cdc_to_bronze")
    .config("spark.jars.packages", f"{ICEBERG},{KAFKA},{AWS},{ICEAWS}")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "rest")
    .config("spark.sql.catalog.lake.uri", "http://localhost:8181")
    .config("spark.sql.catalog.lake.warehouse", "s3://lakehouse/warehouse")
    .config("spark.sql.catalog.lake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lake.s3.endpoint", "http://localhost:9000")
    .config("spark.sql.catalog.lake.s3.path-style-access", "true")
    .config("spark.sql.catalog.lake.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.lake.s3.secret-access-key", "minioadmin")
    .config("spark.sql.catalog.lake.client.region", "us-east-1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
spark.sql("""
CREATE TABLE IF NOT EXISTS lake.bronze.accounts (
  op STRING, account_id LONG, customer_name STRING,
  balance DOUBLE, status STRING, ts_ms LONG
) USING iceberg
""")

acct = StructType([
    StructField("account_id", LongType()),
    StructField("customer_name", StringType()),
    StructField("balance", DoubleType()),
    StructField("status", StringType()),
])
envelope = StructType([
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("before", acct),
    StructField("after", acct),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "banking.public.accounts")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"),
               StructType([StructField("payload", envelope)])).alias("j"))
       .select("j.payload.*")
)

out = parsed.selectExpr(
    "op",
    "coalesce(after.account_id, before.account_id) as account_id",
    "coalesce(after.customer_name, before.customer_name) as customer_name",
    "coalesce(after.balance, before.balance) as balance",
    "coalesce(after.status, before.status) as status",
    "ts_ms",
)

query = (
    out.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://lakehouse/checkpoints/bronze_accounts")
    .trigger(processingTime="60 seconds")
    .toTable("lake.bronze.accounts")
)
query.awaitTermination()