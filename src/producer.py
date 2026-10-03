from kafka import KafkaProducer
import yfinance as yf
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stocks to monitor
stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]

print("Sending live stock data to Kafka...\n")

while True:

    for symbol in stocks:

        ticker = yf.Ticker(symbol)
        data = ticker.history(period="1d", interval="1m")

        if data.empty:
            continue

        latest = data.iloc[-1]

        message = {
            "symbol": symbol,
            "open": float(latest["Open"]),
            "high": float(latest["High"]),
            "low": float(latest["Low"]),
            "close": float(latest["Close"]),
            "volume": int(latest["Volume"]),
            "timestamp": str(latest.name)
        }

        producer.send("stock_prices", message)
        print(message)

    producer.flush()

    print("-" * 60)

    time.sleep(10)