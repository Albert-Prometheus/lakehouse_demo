import argparse
import random
import time
from datetime import datetime, timedelta
from faker import Faker
import psycopg2
from pymongo import MongoClient

# Initialize Faker
fake = Faker()

# Configuration (Update these if your Docker ports/passwords are different)
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "postgres"  # Default DB
PG_USER = "postgres"
PG_PASSWORD = "password"  # CHANGE THIS to your Docker PG password!

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "ecommerce_lake"

def setup_postgres():
    """Create tables in PostgreSQL (Bronze/Silver Layer placeholder)"""
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()

    # Drop tables if they exist for a fresh start
    cur.execute("DROP TABLE IF EXISTS orders;")
    cur.execute("DROP TABLE IF EXISTS users;")

    # Create Users table
    cur.execute("""
        CREATE TABLE users (
            user_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE,
            signup_date TIMESTAMP,
            country VARCHAR(50)
        )
    """)

    # Create Orders table
    cur.execute("""
        CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(user_id),
            product_category VARCHAR(50),
            amount DECIMAL(10, 2),
            order_date TIMESTAMP,
            status VARCHAR(20)
        )
    """)
    conn.commit()
    return conn, cur

def setup_mongo():
    """Connect to MongoDB and setup collections (Bronze Raw Data)"""
    print("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    # Clear existing data
    db.clickstream.drop()
    return db

def generate_data(num_users=100, num_orders=500, num_clicks=2000):
    pg_conn, pg_cur = setup_postgres()
    mongo_db = setup_mongo()

    print(f"Generating {num_users} users...")
    users = []
    for _ in range(num_users):
        signup_date = fake.date_time_between(start_date='-1y', end_date='now')
        pg_cur.execute(
            "INSERT INTO users (name, email, signup_date, country) VALUES (%s, %s, %s, %s) RETURNING user_id",
            (fake.name(), fake.email(), signup_date, fake.country())
        )
        user_id = pg_cur.fetchone()[0]
        users.append({'user_id': user_id, 'signup_date': signup_date})

    print(f"Generating {num_orders} orders...")
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    statuses = ['Completed', 'Completed', 'Completed', 'Processing', 'Cancelled']
    
    for _ in range(num_orders):
        user = random.choice(users)
        # Order date must be after signup date
        order_date = fake.date_time_between(start_date=user['signup_date'], end_date='now')
        pg_cur.execute(
            "INSERT INTO orders (user_id, product_category, amount, order_date, status) VALUES (%s, %s, %s, %s, %s)",
            (user['user_id'], random.choice(categories), round(random.uniform(10.0, 1500.0), 2), order_date, random.choice(statuses))
        )
    
    pg_conn.commit()
    pg_cur.close()
    pg_conn.close()

    print(f"Generating {num_clicks} clickstream events for MongoDB...")
    clickstream_data = []
    event_types = ['page_view', 'add_to_cart', 'search', 'checkout_started']
    devices = ['mobile', 'desktop', 'tablet']
    
    for _ in range(num_clicks):
        user = random.choice(users)
        event_time = fake.date_time_between(start_date=user['signup_date'], end_date='now')
        
        event = {
            "event_id": fake.uuid4(),
            "user_id": user['user_id'],  # Foreign key linking back to Postgres
            "event_type": random.choice(event_types),
            "timestamp": event_time.isoformat(),
            "device": random.choice(devices),
            "metadata": {
                "url": fake.uri_path(),
                "session_duration_sec": random.randint(10, 600)
            }
        }
        clickstream_data.append(event)
    
    # Batch insert into MongoDB
    if clickstream_data:
        mongo_db.clickstream.insert_many(clickstream_data)

    print("Data generation complete! PostgreSQL and MongoDB are now populated.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Mock Data for Data Lakehouse Demo")
    parser.add_argument('--users', type=int, default=100)
    parser.add_argument('--orders', type=int, default=500)
    parser.add_argument('--clicks', type=int, default=2000)
    args = parser.parse_args()
    
    generate_data(args.users, args.orders, args.clicks)
