CREATE EXTERNAL TABLE staging_customers (
    customer_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    address STRING,
    membership_tier STRING,
    last_updated BIGINT
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET;
ALTER TABLE staging_customers ADD PARTITION (load_date='2025-03-01') LOCATION '/data/customers/2025-03-01';

CREATE EXTERNAL TABLE staging_products (
    product_id INT,
    product_name STRING,
    category STRING,
    price FLOAT,
    last_updated BIGINT
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET;
ALTER TABLE staging_products ADD PARTITION (load_date='2025-03-01') LOCATION '/data/products/2025-03-01';

CREATE EXTERNAL TABLE staging_orders (
    order_id INT,
    customer_id INT,
    order_date BIGINT,
    total_amount FLOAT
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET;
ALTER TABLE staging_orders ADD PARTITION (load_date='2025-03-01') LOCATION '/data/orders/2025-03-01';


CREATE EXTERNAL TABLE staging_order_details (
    order_id INT,
    product_id INT,
    quantity INT,
    unit_price FLOAT
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET;
ALTER TABLE staging_order_details ADD PARTITION (load_date='2025-03-01') LOCATION '/data/order_details/2025-03-01';


CREATE TABLE IF NOT EXISTS import_tracking (
    table_name STRING,
    last_value STRING,  -- Store as STRING to handle both DATETIME and INT
    last_load_date STRING
)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dim_customers (
    surrogate_key BIGINT,          -- Unique key for each version
    customer_id INT,               -- Business key
    first_name STRING,
    last_name STRING,
    email STRING,
    address STRING,
    membership_tier STRING,
    effective_date BIGINT,         -- Start of validity (Unix timestamp)
    expiry_date BIGINT,            -- End of validity (Unix timestamp)
    is_active BOOLEAN             -- True for current, False for historical
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS dim_products (
    surrogate_key BIGINT,          -- Unique key for each version
    product_id INT,                -- Business key
    product_name STRING,
    category STRING,
    price FLOAT,
    effective_date BIGINT,         -- Start of validity (Unix timestamp)
    expiry_date BIGINT,            -- End of validity (Unix timestamp)
    is_active BOOLEAN             -- True for current, False for historical
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET;


CREATE TABLE IF NOT EXISTS fact_sales (
    order_id INT,                 -- From orders
    customer_surrogate_key BIGINT, -- From dim_customers
    product_surrogate_key BIGINT,  -- From dim_products
    order_date BIGINT,            -- Unix timestamp from orders
    quantity INT,                 -- From order_details
    unit_price FLOAT,     -- From order_details
    total_amount FLOAT   -- From orders
)
PARTITIONED BY (order_year STRING)  -- Partition by year
STORED AS PARQUET;



INSERT INTO import_tracking
SELECT 'customers', FROM_UNIXTIME(CAST(MAX(last_updated) / 1000 AS BIGINT)), '2025-03-01' FROM staging_customers
UNION ALL
SELECT 'products', FROM_UNIXTIME(CAST(MAX(last_updated) / 1000 AS BIGINT)), '2025-03-01' FROM staging_products
UNION ALL
SELECT 'orders', FROM_UNIXTIME(CAST(MAX(order_date) / 1000 AS BIGINT)), '2025-03-01' FROM staging_orders
UNION ALL
SELECT 'order_details', CAST(MAX(order_id) AS STRING), '2025-03-01' FROM staging_order_details;


