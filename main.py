import ccxt
import json
from kafka import KafkaProducer
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
# Fetching ticker data
def fetch_binance_data(symbol="BTC/USDT"):
    exchange = ccxt.binance({'enableRateLimit': True})
    # Example: fetch trades
    trades = exchange.fetch_trades(symbol)
    return trades

# Publish to Kafka (Producer)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def produce_to_kafka(topic, data):
    # Data is a list of trades or orderbook updates
    for item in data:
        producer.send(topic, value=item)
    producer.flush()



consumer = KafkaConsumer(
    'crypto-ticks',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='crypto-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# InfluxDB
influx_client = InfluxDBClient(host='localhost', port=8086)
influx_client.switch_database('crypto_data')

for message in consumer:
    trade = message.value  # { 'price': ..., 'amount': ..., 'timestamp': ... }
    # Convert the trade into InfluxDB line protocol or JSON format
    data_point = {
        "measurement": "trades",
        "tags": {
            "symbol": trade['symbol'],
            "side": trade['side']
        },
        "time": trade['timestamp'],
        "fields": {
            "price": float(trade['price']),
            "amount": float(trade['amount'])
        }
    }
    influx_client.write_points([data_point])


def main():
    symbol = "BTC/USDT"
    topic = "crypto-ticks"
    trades = fetch_binance_data(symbol)
    produce_to_kafka(topic, trades)

if __name__ == "__main__":
    main()

