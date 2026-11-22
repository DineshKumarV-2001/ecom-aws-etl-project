
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

# --------------------------------------------------
# 1. Initialize Glue
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BUCKET_NAME"]
)

BUCKET = args["BUCKET_NAME"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# 2. Read Silver Parquet files
# --------------------------------------------------

SILVER_PATH = f"s3://{BUCKET}/silver"

customers = spark.read.parquet(
    f"{SILVER_PATH}/customers/"
)

products = spark.read.parquet(
    f"{SILVER_PATH}/products/"
)

orders = spark.read.parquet(
    f"{SILVER_PATH}/orders/"
)

order_items = spark.read.parquet(
    f"{SILVER_PATH}/order_items/"
)

payments = spark.read.parquet(
    f"{SILVER_PATH}/payments/"
)

print("Silver tables loaded successfully")

# --------------------------------------------------
# 3. DAILY SALES
# --------------------------------------------------

order_revenue = (
    order_items
    .withColumn(
        "item_revenue",
        F.col("quantity") * F.col("unit_price")
    )
    .groupBy("order_id")
    .agg(
        F.sum("item_revenue").alias("order_revenue")
    )
)

# Join revenue with orders
gold_daily_sales = (
    orders
    .join(order_revenue, "order_id", "left")
    .withColumn("sales_date", F.to_date("order_date"))
    .groupBy("sales_date")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("order_revenue").alias("total_revenue"),
        F.avg("order_revenue").alias("average_order_value")
    )
    .withColumn("processed_at", F.current_timestamp())
)


# --------------------------------------------------
# 4. PRODUCT PERFORMANCE
# --------------------------------------------------

oi = order_items.alias("oi")
p = products.alias("p")

product_sales = (
    oi.join(
        p,
        F.col("oi.product_id") == F.col("p.product_id"),
        "inner"
    )
    .select(
        F.col("oi.product_id").alias("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("oi.order_id").alias("order_id"),
        F.col("oi.quantity").alias("quantity"),
        F.col("oi.unit_price").alias("unit_price")
    )
    .withColumn(
        "item_revenue",
        F.col("quantity") * F.col("unit_price")
    )
)

gold_product_performance = (
    product_sales
    .groupBy("product_id", "product_name")
    .agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum("item_revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders")
    )
    .withColumn("processed_at", F.current_timestamp())
)

# --------------------------------------------------
# 5. CUSTOMER SUMMARY
# --------------------------------------------------

order_totals = (
    order_items
    .withColumn("item_revenue", F.col("quantity") * F.col("unit_price"))
    .groupBy("order_id")
    .agg(F.sum("item_revenue").alias("order_total"))
)

customer_orders = (
    orders
    .join(order_totals, "order_id", "left")
)

gold_customer_summary = (
    customers
    .join(customer_orders, "customer_id", "left")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("order_total").alias("total_spent"),
        F.max("order_date").alias("last_purchase_date")
    )
    .withColumn("processed_at", F.current_timestamp())
)

# --------------------------------------------------
# 6. Write Gold Parquet files
# --------------------------------------------------

gold_daily_sales.write \
    .mode("overwrite") \
    .parquet(f"s3://{BUCKET}/gold/daily_sales/")

gold_product_performance.write \
    .mode("overwrite") \
    .parquet(f"s3://{BUCKET}/gold/product_performance/")

gold_customer_summary.write \
    .mode("overwrite") \
    .parquet(f"s3://{BUCKET}/gold/customer_summary/")

print("All Gold tables created successfully")

# --------------------------------------------------
# 7. Commit Glue job
# --------------------------------------------------

job.commit()

