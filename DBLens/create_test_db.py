"""
Creates a test SQLite database with intentional issues for testing DBLens.
Covers all 6 checks: slow_queries, missing_indexes, table_bloat,
resource_usage, index_usage, long_running.

Run: python create_test_db.py
"""
import sqlite3
import random
import os
from datetime import datetime, timedelta

DB_PATH = "test_dblens.db"

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ── Use DELETE journal mode (triggers resource_usage warning) ─────────────────
cur.execute("PRAGMA journal_mode=DELETE")
# Small cache (triggers cache coverage warning)
cur.execute("PRAGMA cache_size=50")

print("🔨 Creating tables...")

# NO indexes → triggers missing_index + slow_query + long_running
cur.execute("""
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    country TEXT,
    created_at TEXT,
    status TEXT DEFAULT 'active'
)""")

cur.execute("""
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TEXT
)""")

# Very large table → triggers slow_query + long_running
cur.execute("""
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    event_type TEXT,
    payload TEXT,
    created_at TEXT
)""")

# Well-indexed table → shows up under index_usage (INFO)
cur.execute("""
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    price REAL,
    stock INTEGER
)""")
cur.execute("CREATE INDEX idx_products_category ON products(category)")
cur.execute("CREATE INDEX idx_products_price     ON products(price)")

print("👤 Inserting 10,000 users...")
countries = ["US", "UK", "IN", "DE", "FR", "CA", "AU", "BR"]
users = [
    (i, f"User {i}", f"user{i}@example.com",
     random.choice(countries),
     (datetime.now() - timedelta(days=random.randint(0, 730))).isoformat(),
     random.choice(["active", "inactive", "banned"]))
    for i in range(1, 10_001)
]
cur.executemany("INSERT INTO users VALUES (?,?,?,?,?,?)", users)

print("🛒 Inserting 50,000 orders...")
products_list = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"]
orders = [
    (i, random.randint(1, 10_000),
     random.choice(products_list),
     round(random.uniform(9.99, 1999.99), 2),
     random.choice(["pending", "shipped", "delivered", "cancelled"]),
     (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat())
    for i in range(1, 50_001)
]
cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", orders)

print("📊 Inserting 100,000 events...")
event_types = ["page_view", "click", "purchase", "login", "logout", "error"]
events = [
    (i, random.randint(1, 10_000),
     random.choice(event_types),
     f'{{"session":"{random.randint(1000,9999)}","val":{random.randint(1,100)}}}',
     (datetime.now() - timedelta(seconds=random.randint(0, 86400*30))).isoformat())
    for i in range(1, 100_001)
]
cur.executemany("INSERT INTO events VALUES (?,?,?,?,?)", events)

print("🏪 Inserting 500 products...")
categories = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home"]
prods = [
    (i, f"Product {i}", random.choice(categories),
     round(random.uniform(4.99, 999.99), 2), random.randint(0, 500))
    for i in range(1, 501)
]
cur.executemany("INSERT INTO products VALUES (?,?,?,?,?)", prods)

conn.commit()

# ── Simulate bloat: insert + delete 30,000 rows (triggers table_bloat) ───────
print("💥 Simulating bloat (insert + delete 30,000 rows)...")
bloat = [
    (i + 100_001, random.randint(1, 10_000), "bloat_event", "x" * 200,
     datetime.now().isoformat())
    for i in range(30_000)
]
cur.executemany("INSERT INTO events VALUES (?,?,?,?,?)", bloat)
conn.commit()
cur.execute("DELETE FROM events WHERE event_type='bloat_event'")
conn.commit()
# Do NOT run VACUUM — keeps freelist pages high for DBLens to detect

conn.close()

print(f"""
✅  Database ready: {DB_PATH}

Issues planted:
  🐢 slow_queries    — large unindexed tables (users/orders/events) cause measurable scan time
  🗂️  missing_indexes — users, orders, events have no non-PK indexes
  📦 table_bloat     — 30,000 deleted rows left as freelist pages (no VACUUM)
  ⚙️  resource_usage  — DELETE journal mode + tiny cache size
  🔎 index_usage     — products has 2 indexes (idx_products_category, idx_products_price)
  ⏳ long_running    — cartesian / type-cast scans on 100k-row events table

Run:
  dblens sqlite {DB_PATH}
  dblens sqlite {DB_PATH} --json-pretty
""")
