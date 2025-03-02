from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, unix_timestamp, monotonically_increasing_id, year, from_unixtime, coalesce, max as spark_max
from datetime import datetime

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Daily_SCD_and_Fact_Update") \
    .enableHiveSupport() \
    .getOrCreate()

# Current date for processing (today's date)
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")  # e.g., 2025-03-02

# --- Process dim_customers (SCD Type 2 Incremental Update) ---
staging_customers_df = spark.table("staging_customers").filter(col("load_date") == LOAD_DATE)
dim_customers_df = spark.table("dim_customers").filter(col("is_active") == True)

# Get max surrogate_key from dim_customers
max_key_df = spark.table("dim_customers").agg(spark_max("surrogate_key").alias("max_key"))
max_key = max_key_df.select(coalesce("max_key", lit(0).cast("bigint")).alias("max_key")).collect()[0]["max_key"]

# New or changed customers
new_or_changed_customers = staging_customers_df.join(
    dim_customers_df,
    (staging_customers_df["customer_id"] == dim_customers_df["customer_id"]) &
    (staging_customers_df["last_updated"] > dim_customers_df["effective_date"]),
    "left_anti"
).select(
    (monotonically_increasing_id() + lit(max_key + 1)).alias("surrogate_key"),
    staging_customers_df["customer_id"],
    staging_customers_df["first_name"],
    staging_customers_df["last_name"],
    staging_customers_df["email"],
    staging_customers_df["address"],
    staging_customers_df["membership_tier"],
    staging_customers_df["last_updated"].alias("effective_date"),
    lit(2534022144000).cast("bigint").alias("expiry_date"),  # 9999-12-31
    lit(True).cast("boolean").alias("is_active"),
    lit(LOAD_DATE).alias("load_date")
)

# Records to expire
to_expire_customers = dim_customers_df.join(
    staging_customers_df,
    (dim_customers_df["customer_id"] == staging_customers_df["customer_id"]) &
    (staging_customers_df["last_updated"] > dim_customers_df["effective_date"]),
    "inner"
).select(
    dim_customers_df["surrogate_key"],
    dim_customers_df["customer_id"],
    dim_customers_df["first_name"],
    dim_customers_df["last_name"],
    dim_customers_df["email"],
    dim_customers_df["address"],
    dim_customers_df["membership_tier"],
    dim_customers_df["effective_date"],
    staging_customers_df["last_updated"].alias("expiry_date"),
    lit(False).cast("boolean").alias("is_active"),
    dim_customers_df["load_date"]
)

# Combine and write
final_customers_df = to_expire_customers.union(new_or_changed_customers)
final_customers_df.write \
    .mode("append") \
    .insertInto("dim_customers")

print(f"Updated dim_customers for {LOAD_DATE}")

# --- Process dim_products (SCD Type 2 Incremental Update) ---
staging_products_df = spark.table("staging_products").filter(col("load_date") == LOAD_DATE)
dim_products_df = spark.table("dim_products").filter(col("is_active") == True)

# Get max surrogate_key from dim_products
max_key_df = spark.table("dim_products").agg(spark_max("surrogate_key").alias("max_key"))
max_key = max_key_df.select(coalesce("max_key", lit(0).cast("bigint")).alias("max_key")).collect()[0]["max_key"]

# New or changed products
new_or_changed_products = staging_products_df.join(
    dim_products_df,
    (staging_products_df["product_id"] == dim_products_df["product_id"]) &
    (staging_products_df["last_updated"] > dim_products_df["effective_date"]),
    "left_anti"
).select(
    (monotonically_increasing_id() + lit(max_key + 1)).alias("surrogate_key"),
    staging_products_df["product_id"],
    staging_products_df["product_name"],
    staging_products_df["category"],
    staging_products_df["price"],
    staging_products_df["last_updated"].alias("effective_date"),
    lit(2534022144000).cast("bigint").alias("expiry_date"),  # 9999-12-31
    lit(True).cast("boolean").alias("is_active"),
    lit(LOAD_DATE).alias("load_date")
)

# Records to expire
to_expire_products = dim_products_df.join(
    staging_products_df,
    (dim_products_df["product_id"] == staging_products_df["product_id"]) &
    (staging_products_df["last_updated"] > dim_products_df["effective_date"]),
    "inner"
).select(
    dim_products_df["surrogate_key"],
    dim_products_df["product_id"],
    dim_products_df["product_name"],
    dim_products_df["category"],
    dim_products_df["price"],
    dim_products_df["effective_date"],
    staging_products_df["last_updated"].alias("expiry_date"),
    lit(False).cast("boolean").alias("is_active"),
    dim_products_df["load_date"]
)

# Combine and write
final_products_df = to_expire_products.union(new_or_changed_products)
final_products_df.write \
    .mode("append") \
    .insertInto("dim_products")

print(f"Updated dim_products for {LOAD_DATE}")

# --- Process fact_sales (Daily Incremental Load) ---
staging_orders_df = spark.table("staging_orders").filter(col("load_date") == LOAD_DATE)
staging_order_details_df = spark.table("staging_order_details").filter(col("load_date") == LOAD_DATE)
dim_customers_df = spark.table("dim_customers")  # All partitions for historical lookup
dim_products_df = spark.table("dim_products")    # All partitions for historical lookup

# Join orders with order_details
order_base_df = staging_orders_df.join(
    staging_order_details_df,
    staging_orders_df["order_id"] == staging_order_details_df["order_id"],
    "inner"
).select(
    staging_orders_df["order_id"],
    staging_orders_df["customer_id"],
    staging_order_details_df["product_id"],
    staging_orders_df["order_date"],
    staging_order_details_df["quantity"],
    staging_order_details_df["unit_price"],
    staging_orders_df["total_amount"]
)

# Join with dimensions
fact_sales_df = order_base_df.join(
    dim_customers_df,
    (order_base_df["customer_id"] == dim_customers_df["customer_id"]) &
    (order_base_df["order_date"] >= dim_customers_df["effective_date"]) &
    (order_base_df["order_date"] < dim_customers_df["expiry_date"]) &
    (dim_customers_df["is_active"] == True),
    "inner"
).join(
    dim_products_df,
    (order_base_df["product_id"] == dim_products_df["product_id"]) &
    (order_base_df["order_date"] >= dim_products_df["effective_date"]) &
    (order_base_df["order_date"] < dim_products_df["expiry_date"]) &
    (dim_products_df["is_active"] == True),
    "inner"
).select(
    order_base_df["order_id"],
    dim_customers_df["surrogate_key"].alias("customer_surrogate_key"),
    dim_products_df["surrogate_key"].alias("product_surrogate_key"),
    order_base_df["order_date"],
    order_base_df["quantity"],
    order_base_df["unit_price"],
    order_base_df["total_amount"],
    year(from_unixtime(order_base_df["order_date"])).alias("order_year")
)

# Write to fact_sales
fact_sales_df.write \
    .mode("append") \
    .insertInto("fact_sales")

print(f"Updated fact_sales for {LOAD_DATE}")
spark.stop()