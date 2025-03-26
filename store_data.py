from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    'stock-prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="stock_db",
    user="user",
    password="pass"
)
cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_prices (
        symbol TEXT,
        price FLOAT,
        timestamp TIMESTAMP
    )
""")
conn.commit()

for message in consumer:
    data = message.value
    cursor.execute(
        "INSERT INTO stock_prices (symbol, price, timestamp) VALUES (%s, %s, to_timestamp(%s))",
        (data['symbol'], data['price'], data['timestamp'])
    )
    conn.commit()
    print(f"Stored in DB: {data}")
