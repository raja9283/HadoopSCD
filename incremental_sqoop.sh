#!/bin/bash

# Enable debugging mode to show executed commands
set -x  

# Define load date
LOAD_DATE=$(date +%Y-%m-%d)

# Define MySQL connection details
MYSQL_URL="jdbc:mysql://<MYSQL_HOST>:3306/<DATABASE>"
MYSQL_USER="<MYSQL_USER>"
MYSQL_PASS="<MYSQL_PASS>"

# Function to get last value from Hive tracking table with fallback
get_last_value() {
    TABLE=$1
    LAST_VALUE=$(hive -e "SET hive.cli.print.header=false; SELECT last_value FROM import_tracking WHERE table_name='$TABLE';" 2>/dev/null)
    if [ -z "$LAST_VALUE" ]; then
        if [ "$TABLE" = "order_details" ]; then
            LAST_VALUE="0"  # Default for order_id
        else
            LAST_VALUE="1970-01-01 00:00:00"  # Default for timestamps
        fi
    fi
    echo "$LAST_VALUE"
}

# Define tables to import
TABLES=("customers" "products" "orders" "order_details")

for TABLE in "${TABLES[@]}"
do
    echo "Starting import for table: $TABLE"

    if [ "$TABLE" = "order_details" ]; then
        CHECK_COLUMN="order_id"
    elif [ "$TABLE" = "orders" ]; then
        CHECK_COLUMN="order_date"
    else
        CHECK_COLUMN="last_updated"
    fi

    # Define SPLIT-BY column based on table
    case "$TABLE" in
        customers)
            SPLIT_COLUMN="customer_id"
            ;;
        orders)
            SPLIT_COLUMN="order_id"
            ;;
        products)
            SPLIT_COLUMN="product_id"
            ;;
        order_details)
            SPLIT_COLUMN="order_id"
            ;;
        *)
            echo "No split column defined for $TABLE. Exiting."
            exit 1
            ;;
    esac

    LAST_VALUE=$(get_last_value $TABLE)
    if [ -z "$LAST_VALUE" ]; then
        echo "Error: LAST_VALUE is empty for $TABLE after get_last_value. Exiting."
        exit 1
    fi

    echo "Using check column: $CHECK_COLUMN"
    echo "Using split-by column: $SPLIT_COLUMN"
    echo "Using last-value: $LAST_VALUE"

    # Run Sqoop import
    sqoop import \
      --connect "$MYSQL_URL" \
      --username "$MYSQL_USER" \
      --password "$MYSQL_PASS" \
      --table "$TABLE" \
      --target-dir "/data/$TABLE/$LOAD_DATE" \
      --as-parquetfile \
      --split-by "$SPLIT_COLUMN" \
      --num-mappers 4 \
      --incremental append \
      --check-column "$CHECK_COLUMN" \
      --last-value "$LAST_VALUE"

    if [ $? -eq 0 ]; then
        echo "Sqoop import successful for $TABLE"
        # Add partition to Hive
        echo "Adding partition to Hive for table: $TABLE"
        hive -e "ALTER TABLE staging_$TABLE ADD IF NOT EXISTS PARTITION (load_date='$LOAD_DATE') LOCATION '/data/$TABLE/$LOAD_DATE';"
        if [ $? -ne 0 ]; then
            echo "Error: Failed to add partition for staging_$TABLE"
            exit 1
        fi
    else
        echo "Error: Sqoop import failed for $TABLE"
        exit 1
    fi
done

# Update tracking table
echo "Updating tracking table in Hive"
hive -e "
INSERT OVERWRITE TABLE import_tracking
SELECT 'customers',FROM_UNIXTIME(CAST(MAX(last_updated) / 1000 AS BIGINT)), '$LOAD_DATE' FROM staging_customers
UNION ALL
SELECT 'products', FROM_UNIXTIME(CAST(MAX(last_updated) / 1000 AS BIGINT)), '$LOAD_DATE' FROM staging_products
UNION ALL
SELECT 'orders', FROM_UNIXTIME(CAST(MAX(order_date) / 1000 AS BIGINT)), '$LOAD_DATE' FROM staging_orders
UNION ALL
SELECT 'order_details', CAST(MAX(order_id) AS STRING), '$LOAD_DATE' FROM staging_order_details;
"

if [ $? -eq 0 ]; then
    echo "Tracking table updated successfully"
else
    echo "Error: Failed to update import_tracking table"
    exit 1
fi

echo "Import process completed!"

# Disable debugging mode
set +x