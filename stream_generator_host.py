#!/usr/bin/env python3
"""
Real-Time Streaming Data Generator for Benchmark & Stress Testing
Windows Host Version - 連接到 Docker 容器中的資料庫
"""

import argparse
import random
import time
import threading
import uuid
from datetime import datetime
from faker import Faker
from pymongo import MongoClient
import psycopg2

fake = Faker()

# Windows Host 配置 (連接到 localhost)
MONGO_URI = "mongodb://mongo:admin@localhost:27017/?authSource=admin"
PG_HOST = "localhost"
PG_PORT = "5049"
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "admin"

PRODUCTS = [
    {"id": i, "category": cat, "price": random.randint(500, 50000)}
    for i, cat in enumerate(["Smartphones", "Laptops", "Tablets", "Audio & Headphones", "Smartwatches", "Accessories"] * 10)
]

EVENT_TYPES = ['page_view', 'product_detail', 'add_to_cart', 'search', 'checkout_started']
DEVICES = ['mobile', 'desktop', 'tablet']
PAYMENT_METHODS = ['Credit Card', 'LINE Pay', 'Apple Pay']

class StreamGenerator:
    def __init__(self, tps=100, duration=60):
        self.tps = tps
        self.duration = duration
        self.running = False
        self.events_generated = 0
        
    def generate_event(self):
        product = random.choice(PRODUCTS)
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 500),
            "event_type": random.choice(EVENT_TYPES),
            "timestamp": datetime.now().isoformat(),
            "device": random.choice(DEVICES),
            "metadata": {"product_id": product['id'], "category": product['category']}
        }
    
    def run(self):
        print(f"\n==> Starting Stream Generator: {self.tps} TPS for {self.duration}s")
        
        mongo = MongoClient(MONGO_URI)
        db = mongo['ecommerce_stream']
        
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, database=PG_DB, user=PG_USER, password=PG_PASSWORD)
        
        self.running = True
        start = time.time()
        
        while self.running and (time.time() - start) < self.duration:
            # 寫入 MongoDB
            events = [self.generate_event() for _ in range(max(1, self.tps // 2))]
            if events:
                db.clickstream_stream.insert_many(events, ordered=False)
            
            # 寫入 PostgreSQL (較少)
            cur = pg_conn.cursor()
            for _ in range(max(1, self.tps // 10)):
                product = random.choice(PRODUCTS)
                try:
                    cur.execute("""INSERT INTO orders (user_id, order_date, status, payment_method, shipping_fee, discount_amount) 
                                VALUES (%s, %s, %s, %s, %s, %s) RETURNING order_id""",
                               (random.randint(1, 500), datetime.now(), 'Completed', random.choice(PAYMENT_METHODS), 60, 0))
                    oid = cur.fetchone()[0]
                    cur.execute("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)",
                               (oid, product['id'], random.randint(1,3), product['price']))
                except:
                    pass
            pg_conn.commit()
            cur.close()
            
            self.events_generated += len(events)
            print(f"Generated {self.events_generated} events | TPS: {self.tps} | Elapsed: {time.time()-start:.0f}s")
            time.sleep(1)
        
        mongo.close()
        pg_conn.close()
        print(f"\n==> Complete! Total events: {self.events_generated}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tps', type=int, default=50)
    parser.add_argument('--duration', type=int, default=60)
    args = parser.parse_args()
    
    gen = StreamGenerator(args.tps, args.duration)
    gen.run()
