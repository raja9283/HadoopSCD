-- Incremental Partition Commands (example for one day)
ALTER TABLE staging_customers ADD IF NOT EXISTS PARTITION (load_date='<LOAD_DATE>') LOCATION '/data/customers/<LOAD_DATE>';
ALTER TABLE staging_products ADD IF NOT EXISTS PARTITION (load_date='<LOAD_DATE>') LOCATION '/data/products/<LOAD_DATE>';
ALTER TABLE staging_orders ADD IF NOT EXISTS PARTITION (load_date='<LOAD_DATE>') LOCATION '/data/orders/<LOAD_DATE>';
ALTER TABLE staging_order_details ADD IF NOT EXISTS PARTITION (load_date='<LOAD_DATE>') LOCATION '/data/order_details/<LOAD_DATE>';

-- Note: Replace <LOAD_DATE> with actual date (e.g., '2025-03-02') in incremental_sqoop.sh