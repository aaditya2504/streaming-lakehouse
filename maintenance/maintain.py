from pyspark.sql import SparkSession

S = (SparkSession.builder.appName("maintenance")
     .config("spark.jars.packages",
             "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
             "org.apache.iceberg:iceberg-aws-bundle:1.6.1")
     .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
     .config("spark.sql.catalog.lake","org.apache.iceberg.spark.SparkCatalog")
     .config("spark.sql.catalog.lake.type","rest")
     .config("spark.sql.catalog.lake.uri","http://localhost:8181")
     .config("spark.sql.catalog.lake.warehouse","s3://lakehouse/warehouse")
     .config("spark.sql.catalog.lake.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
     .config("spark.sql.catalog.lake.s3.endpoint","http://localhost:9000")
     .config("spark.sql.catalog.lake.s3.path-style-access","true")
     .config("spark.sql.catalog.lake.client.region","us-east-1")
     .getOrCreate())

T = "lake.bronze.accounts"

before = S.sql(f'SELECT count(*) c FROM lake.bronze.accounts.files').collect()[0]["c"]
print("files BEFORE:", before)

S.sql(f"CALL lake.system.rewrite_data_files(table => '{T}', options => map('target-file-size-bytes','134217728'))").show()
S.sql(f"CALL lake.system.rewrite_manifests('{T}')").show()
S.sql(f"CALL lake.system.expire_snapshots(table => '{T}', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 5)").show()

after = S.sql(f'SELECT count(*) c FROM lake.bronze.accounts.files').collect()[0]["c"]
print("files AFTER:", after)
