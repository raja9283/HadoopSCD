import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
conn = mysql.connector.connect(
    host="<MYSQL_HOST>",
    user="<MYSQL_USER>",
    password="<MYSQL_PASS>",
    database="<DATABASE>"
)
cursor = conn.cursor()

# Generate Products (100)
products = []
categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Toys']
for i in range(1, 101):
    product = (
        i,
        fake.word().capitalize() + " " + fake.word().capitalize(),
        random.choice(categories),
        round(random.uniform(5.99, 199.99), 2),
        fake.date_time_between(start_date="-1y", end_date="now")
    )
    products.append(product)
cursor.executemany("INSERT INTO products (product_id, product_name, category, price, last_updated) VALUES (%s, %s, %s, %s, %s)", products)

# Generate Customers (1000)
customers = []
membership_tiers = ['Basic', 'Premium', 'Platinum']
for i in range(1, 1001):
    customer = (
        i,
        fake.first_name(),
        fake.last_name(),
        fake.email(),
        fake.street_address() + ", " + fake.city(),
        random.choice(membership_tiers),
        fake.date_time_between(start_date="-1y", end_date="now")
    )
    customers.append(customer)
cursor.executemany("INSERT INTO customers (customer_id, first_name, last_name, email, address, membership_tier, last_updated) VALUES (%s, %s, %s, %s, %s, %s, %s)", customers)

# Generate Orders (5000)
orders = []
start_date = datetime(2024, 1, 1)
for i in range(1, 5001):
    order_date = fake.date_time_between(start_date, "now")
    customer_id = random.randint(1, 1000)
    order = (i, customer_id, order_date, 0.00)
    orders.append(order)
cursor.executemany("INSERT INTO orders (order_id, customer_id, order_date, total_amount) VALUES (%s, %s, %s, %s)", orders)

# Generate Order Details (10000)
order_details = []
order_ids = list(range(1, 5001))
random.shuffle(order_ids)
for i in range(10000):
    order_id = order_ids[i % 5000]
    product_id = random.randint(1, 100)
    quantity = random.randint(1, 5)
    unit_price = [p[3] for p in products if p[0] == product_id][0]
    order_details.append((order_id, product_id, quantity, unit_price))
cursor.executemany("INSERT INTO order_details (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)", order_details)

# Update total_amount
cursor.execute("""
    UPDATE orders o
    JOIN (SELECT order_id, SUM(quantity * unit_price) as total FROM order_details GROUP BY order_id) od
    ON o.order_id = od.order_id
    SET o.total_amount = od.total
""")

conn.commit()
print("Initial data loaded: 1000 customers, 100 products, 5000 orders, 10000 order details")
conn.close()