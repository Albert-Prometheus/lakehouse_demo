#!/usr/bin/env python3
"""
Real-Time Streaming Data Generator for Benchmark & Stress Testing
模擬 3C 電商網站的高並發流量，生產:
- Clickstream events (瀏覽、加購物車、結帳)
- Orders (即時訂單)
- Support tickets (客服進線)

Usage:
    python stream_generator.py --mode=continuous --duration=3600 --tps=100
    python stream_generator.py --mode=stress --start-tps=10 --end-tps=1000 --step=100
"""

import argparse
import json
import random
import time
import threading
import uuid
from datetime import datetime, timedelta
from faker import Faker
from pymongo import MongoClient
import psycopg2
import sys

fake = Faker()

# Configuration
MONGO_URI = "mongodb://mongo:admin@mongodb:27017/?authSource=admin"
PG_HOST = "pgvector"
PG_PORT = "5432"
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "admin"

# Product Catalog (預先載入避免重複)
PRODUCTS = [
    {"id": i, "category": cat, "price": random.randint(500, 50000)}
    for i, cat in enumerate([
        "Smartphones", "Laptops", "Tablets", "Audio & Headphones", "Smartwatches", "Accessories"
    ] * 10)
]

EVENT_TYPES = ['page_view', 'product_detail', 'add_to_cart', 'remove_from_cart', 
              'search', 'checkout_started', 'checkout_completed', 'review_read', 'wishlist_add']
PAYMENT_METHODS = ['Credit Card', 'LINE Pay', 'Apple Pay', 'Bank Transfer']
DEVICES = ['mobile', 'desktop', 'tablet']
OS_LIST = ['iOS', 'Android', 'Windows', 'macOS']
REFERRERS = ['google', 'facebook', 'instagram', 'direct', 'email', 'youtube', 'LINE']

class StreamingGenerator:
    def __init__(self, tps=100, duration=0):
        self.tps = tps  # Transactions per second
        self.duration = duration  # 0 = infinite
        self.running = False
        self.events_generated = 0
        self.start_time = None
        self.lock = threading.Lock()
        
    def generate_click_event(self):
        """產生單一點擊事件"""
        product = random.choice(PRODUCTS)
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 500),
            "event_type": random.choice(EVENT_TYPES),
            "timestamp": datetime.now().isoformat(),
            "device": random.choice(DEVICES),
            "os": random.choice(OS_LIST),
            "referrer": random.choice(REFERRERS),
            "metadata": {
                "product_id": product['id'],
                "category": product['category'],
                "price": product['price'],
                "session_duration_sec": random.randint(5, 600),
                "page_depth": random.randint(1, 15)
            }
        }
    
    def generate_order(self):
        """產生即時訂單"""
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        return {
            "order_id": None,  # 寫入 DB 後產生
            "user_id": random.randint(1, 500),
            "product_id": product['id'],
            "quantity": qty,
            "unit_price": product['price'],
            "order_date": datetime.now().isoformat(),
            "status": "Completed",
            "payment_method": random.choice(PAYMENT_METHODS),
            "shipping_fee": random.choice([0, 60, 100, 150]),
            "discount_amount": round(random.uniform(0, 200) if random.random() < 0.3 else 0, 2)
        }
    
    def stream_to_mongodb(self, mongo_db, count=100):
        """批次寫入點擊流到 MongoDB"""
        events = [self.generate_click_event() for _ in range(count)]
        if events:
            mongo_db.clickstream_stream.insert_many(events, ordered=False)
        return len(events)
    
    def stream_to_postgres(self, pg_conn, count=10):
        """批次寫入訂單到 PostgreSQL"""
        cur = pg_conn.cursor()
        inserted = 0
        for _ in range(count):
            order = self.generate_order()
            try:
                cur.execute("""
                    INSERT INTO orders (user_id, order_date, status, payment_method, shipping_fee, discount_amount)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING order_id
                """, (order['user_id'], order['order_date'], order['status'], 
                      order['payment_method'], order['shipping_fee'], order['discount_amount']))
                order_id = cur.fetchone()[0]
                
                # 同時寫入 order_items
                cur.execute("""
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                """, (order_id, order['product_id'], order['quantity'], order['unit_price']))
                inserted += 1
            except Exception as e:
                pass  # 忽略衝突錯誤
        pg_conn.commit()
        cur.close()
        return inserted
    
    def run_continuous(self):
        """持續產生數據流"""
        self.running = True
        self.start_time = time.time()
        
        # 連接資料庫
        mongo = MongoClient(MONGO_URI)
        mongo_db = mongo['ecommerce_stream']
        mongo_db.clickstream_stream.create_index("timestamp")
        
        pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DB, 
            user=PG_USER, password=PG_PASSWORD
        )
        
        print(f"\n{'='*60}")
        print(f"🚀 Starting Continuous Stream Generator")
        print(f"   Target: {self.tps} TPS | Duration: {self.duration}s")
        print(f"{'='*60}\n")
        
        batch_size = max(1, self.tps // 10)  # 每批處理數
        interval = 10 / self.tps if self.tps > 0 else 0.1
        batch_count = 0
        
        while self.running:
            batch_start = time.time()
            
            # 寫入 MongoDB (90% 流量)
            self.stream_to_mongodb(mongo_db, int(batch_size * 0.9))
            
            # 寫入 PostgreSQL (10% 流量)
            self.stream_to_postgres(pg_conn, max(1, int(batch_size * 0.1)))
            
            self.events_generated += batch_size
            batch_count += 1
            
            # 計算實際 TPS
            elapsed = time.time() - self.start_time
            actual_tps = self.events_generated / elapsed if elapsed > 0 else 0
            
            # 每 5 秒輸出進度
            if batch_count % 50 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Generated: {self.events_generated:,} events | Actual TPS: {actual_tps:.1f} | Elapsed: {elapsed:.0f}s")
            
            # 檢查是否超過 duration
            if self.duration > 0 and elapsed >= self.duration:
                self.stop()
            
            # 控制速度
            time.sleep(max(0, interval))
        
        # 清理
        mongo.close()
        pg_conn.close()
        
        total_time = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"✅ Streaming Complete!")
        print(f"   Total Events: {self.events_generated:,}")
        print(f"   Duration: {total_time:.1f}s")
        print(f"   Average TPS: {self.events_generated / total_time:.1f}")
        print(f"{'='*60}\n")
    
    def run_stress_test(self, start_tps, end_tps, step, step_duration=30):
        """壓力測試模式：漸進增加 TPS"""
        self.running = True
        self.start_time = time.time()
        
        mongo = MongoClient(MONGO_URI)
        mongo_db = mongo['ecommerce_stream']
        
        pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DB, 
            user=PG_USER, password=PG_PASSWORD
        )
        
        print(f"\n{'='*60}")
        print(f"🔥 Starting STRESS TEST")
        print(f"   Range: {start_tps} -> {end_tps} TPS (step: {step})")
        print(f"   Each step lasts: {step_duration}s")
        print(f"{'='*60}\n")
        
        tps_levels = list(range(start_tps, end_tps + 1, step))
        results = []
        
        for target_tps in tps_levels:
            self.tps = target_tps
            batch_size = max(1, target_tps // 10)
            interval = 10 / target_tps if target_tps > 0 else 0.1
            
            print(f"\n📊 Testing at {target_tps} TPS...")
            step_start = time.time()
            step_events = 0
            
            while time.time() - step_start < step_duration:
                self.stream_to_mongodb(mongo_db, batch_size)
                step_events += batch_size
                time.sleep(interval)
            
            actual_tps = step_events / step_duration
            results.append({
                "target_tps": target_tps,
                "actual_tps": actual_tps,
                "events": step_events,
                "duration": step_duration
            })
            print(f"   ✅ Target: {target_tps} TPS | Actual: {actual_tps:.1f} TPS | Success Rate: {actual_tps/target_tps*100:.1f}%")
        
        # 輸出結果
        print(f"\n{'='*60}")
        print("📈 BENCHMARK RESULTS")
        print(f"{'Target TPS':<12} {'Actual TPS':<12} {'Success %':<12} {'Status'}")
        print("-" * 50)
        for r in results:
            status = "✅ PASS" if r['actual_tps'] / r['target_tps'] > 0.8 else "❌ FAIL"
            print(f"{r['target_tps']:<12} {r['actual_tps']:<12.1f} {r['actual_tps']/r['target_tps']*100:<12.1f} {status}")
        print(f"{'='*60}\n")
        
        self.running = False
        mongo.close()
        pg_conn.close()
    
    def stop(self):
        self.running = False
        print("\n🛑 Stopping generator...")


def setup_destination_tables():
    """建立目標資料表"""
    print("Setting up destination tables...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, database=PG_DB, 
        user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()
    
    # 確保 order_items 存在
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                item_id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(order_id),
                product_id INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(10,2)
            )
        """)
        conn.commit()
    except:
        pass
    
    cur.close()
    conn.close()
    print("✅ Destination tables ready.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Streaming Data Generator for Benchmark")
    parser.add_argument('--mode', choices=['continuous', 'stress'], default='continuous',
                       help='Generator mode')
    parser.add_argument('--tps', type=int, default=100,
                       help='Target transactions per second (for continuous mode)')
    parser.add_argument('--duration', type=int, default=0,
                       help='Duration in seconds (0 = infinite)')
    parser.add_argument('--start-tps', type=int, default=10,
                       help='Starting TPS (for stress mode)')
    parser.add_argument('--end-tps', type=int, default=500,
                       help='Ending TPS (for stress mode)')
    parser.add_argument('--step', type=int, default=50,
                       help='TPS increment step (for stress mode)')
    
    args = parser.parse_args()
    
    # 確保目標表存在
    setup_destination_tables()
    
    # 建立產生器
    generator = StreamingGenerator(tps=args.tps, duration=args.duration)
    
    if args.mode == 'continuous':
        try:
            generator.run_continuous()
        except KeyboardInterrupt:
            generator.stop()
    elif args.mode == 'stress':
        generator.run_stress_test(args.start_tps, args.end_tps, args.step)
