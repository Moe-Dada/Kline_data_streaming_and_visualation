import time
import json
import ccxt
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'crypto-ticks'
SYMBOL = 'BTC/USDT'
FETCH_INTERVAL = 10  # seconds between fetches

def create_producer():
    """
    Create a Kafka producer with JSON serialization.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_trades():
    """
    Fetch trades using CCXT from Binance as an example.
    Returns a list of trades with relevant fields.
    """
    exchange = ccxt.binance({'enableRateLimit': True})
    trades = exchange.fetch_trades(SYMBOL)
    parsed_trades = []
    for t in trades:
        parsed_trades.append({
            'symbol': t['symbol'],
            'timestamp': t['timestamp'],  # in milliseconds
            'side': t['side'],
            'price': float(t['price']),
            'amount': float(t['amount'])
        })
    return parsed_trades

def main():
    producer = create_producer()
    print(f"[Producer] Starting, publishing trades for {SYMBOL} to Kafka topic '{KAFKA_TOPIC}'...")

    while True:
        try:
            trades = fetch_trades()
            for trade in trades:
                producer.send(KAFKA_TOPIC, value=trade)
            producer.flush()
            print(f"[Producer] Sent {len(trades)} trades to topic '{KAFKA_TOPIC}'.")
            time.sleep(FETCH_INTERVAL)
        except Exception as e:
            print("[Producer] Error:", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
