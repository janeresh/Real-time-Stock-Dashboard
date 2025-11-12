# fetch_data.py
import os, time, json
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf
from kafka import KafkaProducer

# Environment Variables
load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")
TICKERS = [t.strip().upper() for t in os.getenv("TICKERS", "AAPL,MSFT").split(",")]
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "5"))

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    linger_ms=100
)

def fetch_snapshot():
    df = yf.download(
        tickers=" ".join(TICKERS),
        period="1d",
        interval="1m",
        progress=False,
        prepost=False,
    ).tail(1)

    msgs = []
    ts = time.time()
    if isinstance(df.columns, pd.MultiIndex):
        for t in TICKERS:
            row = df.xs(t, axis=1, level=1)
            if row.empty: 
                continue
            price = float(row["Close"].iloc[-1])
            msgs.append({"symbol": t, "price": price, "timestamp": ts})
    else:
        t = TICKERS[0]
        price = float(df["Close"].iloc[-1])
        msgs.append({"symbol": t, "price": price, "timestamp": ts})
    return msgs

def main():
    print(f"Producer started for {TICKERS} → topic '{TOPIC}' ({BOOTSTRAP})")
    while True:
        try:
            msgs = fetch_snapshot()
            for m in msgs:
                producer.send(TOPIC, m)
            producer.flush()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] sent {len(msgs)} msgs")
        except Exception as e:
            print("⚠️ producer error:", e)
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
