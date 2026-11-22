
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# 1. CONFIGURATION

args = getResolvedOptions(sys.argv, ["JOB_NAME","BUCKET_NAME"])
BUCKET = args["BUCKET_NAME"]

TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id"
}


# 2. INITIALIZE GLUE AND SPARK

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# 3. PROCESS TABLES

for table, primary_key in TABLES.items():

    print(f"Processing table: {table}")

    input_path = f"s3://{BUCKET}/bronze/{table}/*/*.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print(f"Source records: {df.count()}")

    # Convert updated_at into timestamp
    df = df.withColumn(
        "updated_at",
        to_timestamp(col("updated_at"))
    )

    # Remove records without primary keys
    df = df.dropna(subset=[primary_key])

    # Keep the latest record for each primary key
    window = Window.partitionBy(primary_key).orderBy(
        col("updated_at").desc_nulls_last()
    )

    df = (
        df.withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    print(f"Clean records: {df.count()}")

    # Write Silver data in Parquet format
    output_path = f"s3://{BUCKET}/silver/{table}/"

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .save(output_path)
    )

    print(f"Completed table: {table}")


# 4. FINISH JOB

job.commit()

print("All tables processed successfully!")

