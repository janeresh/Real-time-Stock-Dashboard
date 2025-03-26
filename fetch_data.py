from kafka import KafkaProducer
import yfinance as yf
import json
import time

kafka_data = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbol = 'AAPL'
ticker = yf.Ticker(symbol)

while True:
    price = ticker.info["regularMarketPrice"]
    data = {
        'symbol': symbol,
        'price': price,
        'timestamp': time.time()
    }
    kafka_data.send('stock-prices', value=data)
    print(f"Sent: {data}")
    time.sleep(5)
