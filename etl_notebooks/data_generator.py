import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from pymongo import MongoClient

fake = Faker()

PG_HOST = "pgvector"
PG_PORT = "5432"
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "admin"

MONGO_URI = "mongodb://mongo:admin@mongodb:27017/?authSource=admin"
MONGO_DB = "ecommerce_lake"

# ============================================================
# 真實 3C 產品目錄 (品牌 + 型號)
# ============================================================
PRODUCT_CATALOG = {
    'Smartphones': [
        ('Apple', 'iPhone 15 Pro Max', 799, 1199), ('Apple', 'iPhone 15', 599, 899),
        ('Samsung', 'Galaxy S24 Ultra', 749, 1099), ('Samsung', 'Galaxy A54', 249, 399),
        ('Google', 'Pixel 8 Pro', 649, 999), ('Sony', 'Xperia 1 V', 699, 1049),
        ('Xiaomi', 'Xiaomi 14', 399, 649), ('OPPO', 'Find X7 Ultra', 549, 849),
    ],
    'Laptops': [
        ('Apple', 'MacBook Pro 16" M3 Max', 2199, 3499), ('Apple', 'MacBook Air 15" M3', 999, 1499),
        ('ASUS', 'ROG Zephyrus G16', 1299, 1999), ('ASUS', 'ZenBook 14 OLED', 799, 1199),
        ('Lenovo', 'ThinkPad X1 Carbon Gen 11', 1099, 1699), ('Lenovo', 'Legion Pro 7i', 1399, 2199),
        ('Dell', 'XPS 15', 999, 1599), ('HP', 'Spectre x360 16', 1099, 1699),
        ('MSI', 'Creator Z17', 1499, 2399), ('Acer', 'Swift X 16', 699, 1099),
    ],
    'Tablets': [
        ('Apple', 'iPad Pro 12.9" M2', 899, 1299), ('Apple', 'iPad Air M2', 449, 699),
        ('Samsung', 'Galaxy Tab S9 Ultra', 849, 1199), ('Samsung', 'Galaxy Tab S9 FE', 329, 499),
        ('Lenovo', 'Tab P12 Pro', 499, 749), ('Microsoft', 'Surface Pro 9', 799, 1199),
    ],
    'Audio & Headphones': [
        ('Apple', 'AirPods Pro 2', 169, 249), ('Apple', 'AirPods Max', 389, 549),
        ('Sony', 'WH-1000XM5', 249, 349), ('Sony', 'WF-1000XM5', 199, 279),
        ('Bose', 'QuietComfort Ultra', 279, 429), ('JBL', 'Tour One M2', 199, 299),
        ('Sennheiser', 'Momentum 4', 249, 349), ('Samsung', 'Galaxy Buds2 Pro', 149, 229),
    ],
    'Smartwatches': [
        ('Apple', 'Apple Watch Ultra 2', 599, 799), ('Apple', 'Apple Watch Series 9', 299, 429),
        ('Samsung', 'Galaxy Watch 6 Classic', 299, 449), ('Garmin', 'Fenix 7X', 549, 799),
        ('Google', 'Pixel Watch 2', 249, 349), ('Fitbit', 'Sense 2', 199, 299),
    ],
    'Accessories': [
        ('Logitech', 'MX Master 3S', 69, 99), ('Logitech', 'MX Keys Mini', 69, 99),
        ('Anker', 'PowerCore 26800 PD', 39, 69), ('Apple', 'MagSafe Charger', 29, 39),
        ('Samsung', 'T7 Shield SSD 2TB', 119, 179), ('SanDisk', 'Extreme Pro 1TB', 89, 139),
        ('Belkin', 'Thunderbolt 4 Dock', 249, 379), ('Razer', 'DeathAdder V3', 59, 89),
    ],
}

SUPPLIERS = [
    ('TechWorld Distribution', 'Taipei', 'A'), ('Global Electronics Co.', 'Shenzhen', 'A'),
    ('Pacific Digital Supply', 'Tokyo', 'B'), ('StarTech Wholesale', 'Seoul', 'A'),
    ('EuroTech Partners', 'Berlin', 'B'), ('SilkRoad Trading', 'Hong Kong', 'C'),
    ('Alliance Components', 'Singapore', 'B'), ('NorthStar Imports', 'Los Angeles', 'C'),
]


def setup_postgres():
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()

    # Drop all tables
    for t in ['order_items', 'returns', 'purchase_orders', 'orders', 'products', 'suppliers', 'users']:
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE;")

    cur.execute("""
        CREATE TABLE suppliers (
            supplier_id SERIAL PRIMARY KEY,
            name VARCHAR(150),
            city VARCHAR(50),
            rating VARCHAR(5),
            lead_time_days INTEGER,
            on_time_rate DECIMAL(5,2)
        )
    """)

    cur.execute("""
        CREATE TABLE products (
            product_id SERIAL PRIMARY KEY,
            sku VARCHAR(30) UNIQUE,
            brand VARCHAR(50),
            name VARCHAR(200),
            category VARCHAR(50),
            cost_price DECIMAL(10,2),
            selling_price DECIMAL(10,2),
            stock_quantity INTEGER,
            reorder_point INTEGER,
            supplier_id INTEGER REFERENCES suppliers(supplier_id)
        )
    """)

    cur.execute("""
        CREATE TABLE users (
            user_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(150) UNIQUE,
            phone VARCHAR(30),
            city VARCHAR(80),
            signup_date TIMESTAMP,
            member_level VARCHAR(20),
            total_points INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(user_id),
            order_date TIMESTAMP,
            status VARCHAR(20),
            payment_method VARCHAR(30),
            shipping_fee DECIMAL(8,2),
            discount_amount DECIMAL(8,2)
        )
    """)

    cur.execute("""
        CREATE TABLE order_items (
            item_id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES orders(order_id),
            product_id INTEGER REFERENCES products(product_id),
            quantity INTEGER,
            unit_price DECIMAL(10,2)
        )
    """)

    cur.execute("""
        CREATE TABLE returns (
            return_id SERIAL PRIMARY KEY,
            order_id INTEGER REFERENCES orders(order_id),
            product_id INTEGER REFERENCES products(product_id),
            return_date TIMESTAMP,
            reason VARCHAR(100),
            refund_amount DECIMAL(10,2),
            status VARCHAR(20)
        )
    """)

    cur.execute("""
        CREATE TABLE purchase_orders (
            po_id SERIAL PRIMARY KEY,
            supplier_id INTEGER REFERENCES suppliers(supplier_id),
            product_id INTEGER REFERENCES products(product_id),
            quantity INTEGER,
            unit_cost DECIMAL(10,2),
            order_date TIMESTAMP,
            expected_date TIMESTAMP,
            received_date TIMESTAMP,
            status VARCHAR(20)
        )
    """)

    conn.commit()
    return conn, cur


def setup_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    db.clickstream.drop()
    db.support_tickets.drop()
    return db


def generate_data():
    pg_conn, pg_cur = setup_postgres()
    mongo_db = setup_mongo()

    # ---- 1. Suppliers ----
    supplier_ids = []
    for name, city, rating in SUPPLIERS:
        lead_time = random.randint(3, 21)
        on_time = round(random.uniform(0.7, 0.99), 2)
        pg_cur.execute(
            "INSERT INTO suppliers (name, city, rating, lead_time_days, on_time_rate) VALUES (%s,%s,%s,%s,%s) RETURNING supplier_id",
            (name, city, rating, lead_time, on_time)
        )
        supplier_ids.append(pg_cur.fetchone()[0])
    pg_conn.commit()
    print(f"  [OK] {len(supplier_ids)} Suppliers created.")

    # ---- 2. Products ----
    product_map = {}  # product_id -> {category, cost, price, ...}
    product_ids = []
    sku_counter = 1000
    for category, items in PRODUCT_CATALOG.items():
        for brand, model, cost_low, cost_high in items:
            cost = round(random.uniform(cost_low, cost_high * 0.65), 2)
            price = round(cost * random.uniform(1.25, 1.85), 2)
            stock = random.randint(20, 800)
            reorder = random.randint(10, 50)
            sku = f"SKU-{sku_counter}"
            sku_counter += 1
            sup_id = random.choice(supplier_ids)
            pg_cur.execute(
                "INSERT INTO products (sku, brand, name, category, cost_price, selling_price, stock_quantity, reorder_point, supplier_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING product_id",
                (sku, brand, model, category, cost, price, stock, reorder, sup_id)
            )
            pid = pg_cur.fetchone()[0]
            product_ids.append(pid)
            product_map[pid] = {'category': category, 'brand': brand, 'cost': cost, 'price': price, 'name': model}
    pg_conn.commit()
    print(f"  [OK] {len(product_ids)} Products created.")

    # ---- 3. Users (500) ----
    levels = ['Standard', 'Silver', 'Gold', 'VIP']
    weights = [0.55, 0.25, 0.13, 0.07]
    payments = ['Credit Card', 'Debit Card', 'LINE Pay', 'Apple Pay', 'Bank Transfer', 'Cash on Delivery']
    cities_tw = ['Taipei', 'New Taipei', 'Taichung', 'Kaohsiung', 'Tainan', 'Taoyuan', 'Hsinchu', 'Keelung', 'Changhua', 'Pingtung']
    users = []
    for i in range(500):
        signup_date = fake.date_time_between(start_date='-2y', end_date='-7d')
        level = random.choices(levels, weights=weights)[0]
        pg_cur.execute(
            "INSERT INTO users (name, email, phone, city, signup_date, member_level, total_points) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING user_id",
            (fake.name(), f"user{i}_{fake.uuid4()[:8]}@mail.com", fake.phone_number(),
             random.choice(cities_tw), signup_date, level, random.randint(0, 50000))
        )
        users.append({'user_id': pg_cur.fetchone()[0], 'signup_date': signup_date, 'level': level})
    pg_conn.commit()
    print(f"  [OK] {len(users)} Users created.")

    # ---- 4. Orders + Order Items (3500 orders) ----
    statuses_w = ['Completed'] * 82 + ['Processing'] * 8 + ['Shipped'] * 5 + ['Cancelled'] * 5
    order_records = []
    for _ in range(3500):
        user = random.choice(users)
        order_date = fake.date_time_between(start_date=user['signup_date'], end_date='now')
        status = random.choice(statuses_w)
        payment = random.choice(payments)
        shipping = round(random.choice([0, 0, 0, 60, 100, 150]), 2)
        discount = round(random.uniform(0, 200) if random.random() < 0.3 else 0, 2)

        pg_cur.execute(
            "INSERT INTO orders (user_id, order_date, status, payment_method, shipping_fee, discount_amount) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING order_id",
            (user['user_id'], order_date, status, payment, shipping, discount)
        )
        oid = pg_cur.fetchone()[0]

        # Each order has 1~4 line items
        num_items = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05])[0]
        chosen_products = random.sample(product_ids, min(num_items, len(product_ids)))
        for pid in chosen_products:
            qty = random.randint(1, 3)
            price = product_map[pid]['price']
            pg_cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)",
                (oid, pid, qty, price)
            )

        order_records.append({'order_id': oid, 'order_date': order_date, 'status': status,
                              'products': chosen_products, 'user_id': user['user_id']})
    pg_conn.commit()
    print(f"  [OK] 3500 Orders with line items created.")

    # ---- 5. Returns (roughly 8% of completed orders) ----
    completed_orders = [o for o in order_records if o['status'] == 'Completed']
    return_reasons = ['Product defective', 'Wrong item shipped', 'Not as described',
                      'Changed mind', 'Better price elsewhere', 'Arrived too late']
    return_count = 0
    for order in random.sample(completed_orders, int(len(completed_orders) * 0.08)):
        pid = random.choice(order['products'])
        return_date = order['order_date'] + timedelta(days=random.randint(1, 30))
        if return_date > datetime.now():
            return_date = datetime.now()
        refund = round(product_map[pid]['price'] * random.uniform(0.8, 1.0), 2)
        pg_cur.execute(
            "INSERT INTO returns (order_id, product_id, return_date, reason, refund_amount, status) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (order['order_id'], pid, return_date, random.choice(return_reasons), refund,
             random.choice(['Approved', 'Approved', 'Pending']))
        )
        return_count += 1
    pg_conn.commit()
    print(f"  [OK] {return_count} Returns created.")

    # ---- 6. Purchase Orders (from Suppliers) ----
    po_count = 0
    for _ in range(200):
        sup_id = random.choice(supplier_ids)
        pid = random.choice(product_ids)
        qty = random.randint(50, 500)
        unit_cost = product_map[pid]['cost']
        po_date = fake.date_time_between(start_date='-2y', end_date='now')
        expected = po_date + timedelta(days=random.randint(5, 25))
        received = expected + timedelta(days=random.randint(-3, 10)) if random.random() < 0.85 else None
        status = 'Received' if received else random.choice(['In Transit', 'Ordered'])
        pg_cur.execute(
            "INSERT INTO purchase_orders (supplier_id, product_id, quantity, unit_cost, order_date, expected_date, received_date, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (sup_id, pid, qty, unit_cost, po_date, expected, received, status)
        )
        po_count += 1
    pg_conn.commit()
    print(f"  [OK] {po_count} Purchase Orders created.")

    pg_cur.close()
    pg_conn.close()

    # ---- 7. MongoDB: Clickstream (8000 events) ----
    event_types = ['page_view', 'product_detail', 'add_to_cart', 'remove_from_cart',
                   'search', 'checkout_started', 'checkout_completed', 'review_read', 'wishlist_add']
    search_terms = ['iPhone', 'MacBook', 'gaming laptop', 'earbuds', 'charger', 'Samsung', 'tablet',
                    'smartwatch', 'keyboard', 'mouse', 'SSD', 'monitor', 'USB-C hub', 'AirPods']
    clicks = []
    for _ in range(8000):
        user = random.choice(users)
        ts = fake.date_time_between(start_date=user['signup_date'], end_date='now')
        evt = random.choice(event_types)
        doc = {
            "event_id": fake.uuid4(),
            "user_id": user['user_id'],
            "event_type": evt,
            "timestamp": ts.isoformat(),
            "device": random.choice(['mobile', 'desktop', 'tablet']),
            "os": random.choice(['iOS', 'Android', 'Windows', 'macOS']),
            "referrer": random.choice(['google', 'facebook', 'instagram', 'direct', 'email', 'youtube']),
            "metadata": {
                "product_id_viewed": random.choice(product_ids) if evt in ['product_detail', 'add_to_cart', 'wishlist_add'] else None,
                "search_query": random.choice(search_terms) if evt == 'search' else None,
                "session_duration_sec": random.randint(5, 900),
                "page_depth": random.randint(1, 12),
            }
        }
        clicks.append(doc)
    mongo_db.clickstream.insert_many(clicks)
    print(f"  [OK] 8000 Clickstream events -> MongoDB.")

    # ---- 8. MongoDB: Support Tickets (300) ----
    ticket_types = ['Product Inquiry', 'Order Status', 'Return Request', 'Technical Support',
                    'Billing Issue', 'Delivery Problem', 'Warranty Claim']
    tickets = []
    for _ in range(300):
        user = random.choice(users)
        ts = fake.date_time_between(start_date=user['signup_date'], end_date='now')
        tickets.append({
            "ticket_id": fake.uuid4(),
            "user_id": user['user_id'],
            "type": random.choice(ticket_types),
            "priority": random.choice(['Low', 'Medium', 'High', 'Urgent']),
            "status": random.choice(['Open', 'In Progress', 'Resolved', 'Resolved', 'Resolved', 'Closed']),
            "created_at": ts.isoformat(),
            "resolved_at": (ts + timedelta(hours=random.randint(1, 72))).isoformat() if random.random() < 0.75 else None,
            "satisfaction_score": random.choice([1, 2, 3, 4, 5, 5, 4, 4, 5]) if random.random() < 0.6 else None,
        })
    mongo_db.support_tickets.insert_many(tickets)
    print(f"  [OK] 300 Support Tickets -> MongoDB.")

    print("\n=== 3C E-Commerce Full Data Generation Completed! ===")


if __name__ == "__main__":
    generate_data()
