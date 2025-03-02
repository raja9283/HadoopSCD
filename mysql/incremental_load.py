import mysql.connector
from faker import Faker
import random
from datetime import datetime

fake = Faker()
conn = mysql.connector.connect(
    host="<MYSQL_HOST>",
    user="<MYSQL_USER>",
    password="<MYSQL_PASS>",
    database="<DATABASE>"
)
cursor = conn.cursor()

# Get max order_id
cursor.execute("SELECT MAX(order_id) FROM orders")
max_order_id = cursor.fetchone()[0] or 5000

# Get customer_ids and product_ids
cursor.execute("SELECT customer_id FROM customers")
customer_ids = [row[0] for row in cursor.fetchall()]
cursor.execute("SELECT product_id FROM products")
product_ids = [row[0] for row in cursor.fetchall()]

# Update 50 random customers
cursor.execute("SELECT customer_id FROM customers")
customer_ids_update = [row[0] for row in cursor.fetchall()]
random.shuffle(customer_ids_update)
update_customers = customer_ids_update[:50]
membership_tiers = ['Basic', 'Premium', 'Platinum']
for customer_id in update_customers:
    change_type = random.choice(['address', 'membership', 'both'])
    if change_type in ['address', 'both']:
        new_address = fake.street_address() + ", " + fake.city()
        cursor.execute("UPDATE customers SET address = %s, last_updated = NOW() WHERE customer_id = %s", (new_address, customer_id))
    if change_type in ['membership', 'both']:
        new_tier = random.choice(membership_tiers)
        cursor.execute("UPDATE customers SET membership_tier = %s, last_updated = NOW() WHERE customer_id = %s", (new_tier, customer_id))

# Update 10 random products
cursor.execute("SELECT product_id FROM products")
product_ids_update = [row[0] for row in cursor.fetchall()]
random.shuffle(product_ids_update)
update_products = product_ids_update[:10]
for product_id in update_products:
    new_price = round(random.uniform(5.99, 199.99), 2)
    cursor.execute("UPDATE products SET price = %s, last_updated = NOW() WHERE product_id = %s", (new_price, product_id))

# Generate 50 new orders
new_orders = []
start_date = datetime(2025, 3, 1)
for i in range(50):
    order_id = max_order_id + i + 1
    customer_id = random.choice(customer_ids)
    order_date = fake.date_time_between(start_date=start_date, end_date="now")
    total_amount = 0.00
    new_orders.append((order_id, customer_id, order_date, total_amount))
cursor.executemany("INSERT INTO orders (order_id, customer_id, order_date, total_amount) VALUES (%s, %s, %s, %s)", new_orders)

# Generate 100 new order_details
new_order_details = []
for order in new_orders:
    order_id = order[0]
    num_details = random.randint(1, 3)
    for _ in range(num_details):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5.99, 199.99), 2)
        new_order_details.append((order_id, product_id, quantity, unit_price))
cursor.executemany("INSERT INTO order_details (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)", new_order_details)

# Update total_amount
cursor.execute("""
    UPDATE orders o
    JOIN (SELECT order_id, SUM(quantity * unit_price) as total FROM order_details WHERE order_id > %s GROUP BY order_id) od
    ON o.order_id = od.order_id
    SET o.total_amount = od.total
    WHERE o.order_id > %s
""", (max_order_id, max_order_id))

conn.commit()
print(f"Added 50 new orders and 100 new order details to MySQL")
conn.close()