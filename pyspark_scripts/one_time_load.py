from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, unix_timestamp, monotonically_increasing_id, year, from_unixtime

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Initial_Load_SCD_and_Fact") \
    .enableHiveSupport() \
    .getOrCreate()

# Load date for initial load
LOAD_DATE = "2025-03-01"

# --- Process dim_customers (SCD Type 2 Initial Load) ---
staging_customers_df = spark.table("staging_customers").filter(col("load_date") == LOAD_DATE)

dim_customers_df = staging_customers_df.select(
    monotonically_increasing_id().alias("surrogate_key"),
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("address"),
    col("membership_tier"),
    col("last_updated").alias("effective_date"),
    lit(2534022144000).cast("bigint").alias("expiry_date"),  # 9999-12-31
    lit(True).cast("boolean").alias("is_active"),
    lit(LOAD_DATE).alias("load_date")
)

dim_customers_df.write \
    .mode("append") \
    .insertInto("dim_customers")

print("Initial load completed for dim_customers")

# --- Process dim_products (SCD Type 2 Initial Load) ---
staging_products_df = spark.table("staging_products").filter(col("load_date") == LOAD_DATE)

dim_products_df = staging_products_df.select(
    monotonically_increasing_id().alias("surrogate_key"),
    col("product_id"),
    col("product_name"),
    col("category"),
    col("price"),
    col("last_updated").alias("effective_date"),
    lit(2534022144000).cast("bigint").alias("expiry_date"),  # 9999-12-31
    lit(True).cast("boolean").alias("is_active"),
    lit(LOAD_DATE).alias("load_date")
)

dim_products_df.write \
    .mode("append") \
    .insertInto("dim_products")

print("Initial load completed for dim_products")

# --- Process fact_sales (Initial Load) ---
staging_orders_df = spark.table("staging_orders").filter(col("load_date") == LOAD_DATE)
staging_order_details_df = spark.table("staging_order_details").filter(col("load_date") == LOAD_DATE)
dim_customers_df = spark.table("dim_customers").filter(col("load_date") == LOAD_DATE)
dim_products_df = spark.table("dim_products").filter(col("load_date") == LOAD_DATE)

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

# Join with dimensions to get surrogate keys
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

fact_sales_df.write \
    .mode("append") \
    .insertInto("fact_sales")

print("Initial load completed for fact_sales")
spark.stop()