import os, time, json, random, math
from datetime import datetime, timezone
import pandas as pd
from kafka import KafkaProducer

brokers = os.getenv("KAFKA_BROKERS","redpanda:9092")
topic = os.getenv("KAFKA_TICKS_TOPIC","ticks")
producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode())

tickers = ["AAPL","MSFT","AMZN"]
bases = {"AAPL":195.0,"MSFT":415.0,"AMZN":175.0}

print(f"Producing ticks to {topic} on {brokers}... Ctrl+C to stop.")
try:
    while True:
        for t in tickers:
            base = bases[t]
            price = base + random.gauss(0, 0.2)
            size = max(1, int(abs(random.gauss(50, 20))))
            msg = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "ticker": t,
                "price": round(price, 4),
                "size": size
            }
            producer.send(topic, msg)
        producer.flush()
        time.sleep(0.2)
except KeyboardInterrupt:
    print("Stopped.")
