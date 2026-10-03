from kafka import KafkaProducer
import yfinance as yf
import json
import time
import random
from datetime import datetime, timezone


# --------------------------------------------------
# 1. Create Kafka Producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# --------------------------------------------------
# 2. Stocks to monitor
# --------------------------------------------------

stocks = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA"]


# --------------------------------------------------
# 3. Get initial prices from Yahoo Finance
# --------------------------------------------------

current_prices = {}

print("Fetching initial stock prices...\n")


for symbol in stocks:

    ticker = yf.Ticker(symbol)

    data = ticker.history(
        period="1d",
        interval="1m"
    )

    if data.empty:
        print(f"No data available for {symbol}")
        continue

    latest = data.iloc[-1]

    current_prices[symbol] = {
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
        "volume": int(latest["Volume"])
    }

    print(
        f"{symbol}: "
        f"${current_prices[symbol]['close']:.2f}"
    )


# --------------------------------------------------
# 4. Start real-time event generation
# --------------------------------------------------

print("\nStarting real-time stock streaming...\n")


while True:

    for symbol in stocks:

        if symbol not in current_prices:
            continue

        stock = current_prices[symbol]

        # ------------------------------------------
        # Generate small price movement
        # ------------------------------------------

        previous_close = stock["close"]

        price_change = random.uniform(
            -0.002,
            0.002
        )

        new_close = previous_close * (
            1 + price_change
        )

        stock["close"] = new_close

        # Update high and low

        stock["high"] = max(
            stock["high"],
            new_close
        )

        stock["low"] = min(
            stock["low"],
            new_close
        )

        # Increase volume

        stock["volume"] += random.randint(
            1000,
            50000
        )


        # ------------------------------------------
        # Create streaming event
        # ------------------------------------------

        message = {

            "symbol": symbol,

            "open": round(
                stock["open"],
                4
            ),

            "high": round(
                stock["high"],
                4
            ),

            "low": round(
                stock["low"],
                4
            ),

            "close": round(
                stock["close"],
                4
            ),

            "volume": stock["volume"],

            "timestamp": datetime.now(
                timezone.utc
            ).isoformat()
        }


        # ------------------------------------------
        # Send message to Kafka
        # ------------------------------------------

        producer.send(
            "stock_prices",
            message
        )

        print(message)


    producer.flush()

    print("-" * 70)

    # Generate a new batch every 10 seconds

    time.sleep(10)