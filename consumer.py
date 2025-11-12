# store_data.py
import os, json, time, signal, sys
from contextlib import closing
from collections import Counter
from dotenv import load_dotenv
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv()

# --- Config ---
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")
GROUP = os.getenv("KAFKA_GROUP", "stock-consumer-1")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB   = os.getenv("POSTGRES_DB", "stock_db")
PG_USER = os.getenv("POSTGRES_USER", "user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "pass")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_SECONDS = int(os.getenv("FLUSH_SECONDS", "5"))

def to_seconds(ts):
    try:
        t = float(ts)
        return t/1000.0 if t > 1e12 else t  # ms → s if needed
    except Exception:
        return None

def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol TEXT NOT NULL,
            price  DOUBLE PRECISION,
            timestamp TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (symbol, timestamp)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_ts
        ON stock_prices(symbol, timestamp)
    """)

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id=GROUP,
        enable_auto_commit=True,
        auto_offset_reset="earliest",  # first run: read anything already there
        consumer_timeout_ms=0
    )

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = False

    with closing(conn), conn.cursor() as cur:
        ensure_table(cur)
        conn.commit()
        print(f"✅ Connected to Postgres '{PG_DB}', ensured table stock_prices.")
        print(f"✅ Subscribed to Kafka topic '{TOPIC}' at {BOOTSTRAP} (group: {GROUP}).")

        buffer = []
        last_flush = time.time()
        seen = 0

        def flush():
            nonlocal buffer
            if not buffer:
                return
            # upsert to avoid duplicates on (symbol,timestamp)
            try:
                execute_batch(
                    cur,
                    """
                    INSERT INTO stock_prices(symbol, price, timestamp)
                    VALUES (%s, %s, to_timestamp(%s))
                    ON CONFLICT (symbol, timestamp) DO NOTHING
                    """,
                    buffer,
                    page_size=min(len(buffer), 1000)
                )
                conn.commit()
                counts = Counter(b[0] for b in buffer)
                print(f"💾 Inserted {len(buffer)} rows. breakdown={dict(counts)}")
            except Exception as e:
                conn.rollback()
                print("⚠️ insert failed, rolled back:", e)
            finally:
                buffer = []

        def handle_stop(sig, frame):
            print("\n🛑 stopping, flushing…")
            flush()
            try:
                consumer.close()
            finally:
                conn.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_stop)
        signal.signal(signal.SIGTERM, handle_stop)

        while True:
            # poll up to 3s; returns dict of {TopicPartition: [messages]}
            records = consumer.poll(timeout_ms=3000, max_records=1000)

            # heartbeat if nothing arrived
            if not records:
                # time-based flush, so you still see activity
                if (time.time() - last_flush) >= FLUSH_SECONDS:
                    flush()
                    last_flush = time.time()
                    print("⏳ no messages yet… (still listening)")
                continue

            # process all partitions
            for tp, msgs in records.items():
                for msg in msgs:
                    data = msg.value
                    sym = data.get("symbol")
                    price = data.get("price")
                    ts = data.get("timestamp")
                    if sym is None or price is None or ts is None:
                        continue
                    # normalize seconds/ms
                    ts = (float(ts) / 1000.0) if float(ts) > 1e12 else float(ts)
                    buffer.append((sym, float(price), ts))
                    seen += 1

            # print progress + flush by size or time
            print(f"📨 received {seen} msgs (buffer={len(buffer)})")
            if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SECONDS:
                flush()
                last_flush = time.time()

if __name__ == "__main__":
    main()
