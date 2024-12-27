import json
import time
from kafka import KafkaConsumer
from influxdb import InfluxDBClient

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'crypto-ticks'
GROUP_ID = 'crypto-group-ohlc'

INFLUXDB_HOST = 'localhost'  # or 'influxdb' if inside Docker
INFLUXDB_PORT = 8086
INFLUXDB_DBNAME = 'crypto_data'
INFLUXDB_USER = 'admin'
INFLUXDB_PASS = 'admin123'

# Global in-memory structure:
# bars[(symbol, minuteBucket)] = { 'open':..., 'high':..., 'low':..., 'close':..., 'volume':..., 'updated': timestamp_ms }
bars = {}

FLUSH_DELAY_MS = 20000  # how long after a minute is over we consider it "finalized"

def update_bar(trade):
    """
    Update the in-memory bar for the trade's timestamp (minute bucket).
    """
    symbol = trade['symbol']
    ts = trade['timestamp']  # ms
    price = float(trade['price'])
    amount = float(trade['amount'])

    # Determine the minute bucket (truncate ms to the start of the minute).
    minute_bucket = (ts // 60000) * 60000

    key = (symbol, minute_bucket)
    bar = bars.get(key)

    if bar is None:
        # create new bar
        bar = {
            'symbol': symbol,
            'start_ts': minute_bucket,  # start of the minute in ms
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': amount,
            'updated': ts  # last trade timestamp in this bucket
        }
        bars[key] = bar
    else:
        # update existing bar
        if price > bar['high']:
            bar['high'] = price
        if price < bar['low']:
            bar['low'] = price
        bar['close'] = price
        bar['volume'] += amount
        bar['updated'] = ts

def flush_old_bars(influx_client, current_ts):
    """
    Write bars to InfluxDB if the current time is well past
    the bar's minute end (plus FLUSH_DELAY_MS).
    """
    to_delete = []
    for (symbol, minute_bucket), bar in bars.items():
        # The minute ends at minute_bucket + 60,000 ms
        bar_end = minute_bucket + 60000
        # If current_ts is more than bar_end + some delay, we flush
        if current_ts > (bar_end + FLUSH_DELAY_MS):
            # Write to InfluxDB
            influx_point = {
                "measurement": "ohlc_1m",
                "tags": {
                    "symbol": symbol
                },
                "time": bar['start_ts'],  # or you could do bar_end if you prefer
                "fields": {
                    "open": float(bar['open']),
                    "high": float(bar['high']),
                    "low": float(bar['low']),
                    "close": float(bar['close']),
                    "volume": float(bar['volume'])
                }
            }
            try:
                influx_client.write_points([influx_point])
            except Exception as e:
                print("[Consumer] Error writing OHLC to InfluxDB:", e)
            else:
                print(f"[Consumer] Flushed bar => {symbol} @ {minute_bucket} => OHLC: {bar}")
            # Mark for deletion
            to_delete.append((symbol, minute_bucket))

    # Remove flushed bars from memory
    for key in to_delete:
        del bars[key]

def main():
    # Set up Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"[Consumer] Connected to Kafka topic: {KAFKA_TOPIC}")

    # Connect to InfluxDB
    influx_client = InfluxDBClient(
        host=INFLUXDB_HOST,
        port=INFLUXDB_PORT,
        username=INFLUXDB_USER,
        password=INFLUXDB_PASS
    )
    influx_client.switch_database(INFLUXDB_DBNAME)
    print(f"[Consumer] Connected to InfluxDB database: {INFLUXDB_DBNAME}")

    # Main loop
    last_flush_time = time.time()
    FLUSH_INTERVAL_SEC = 5  # how often we check for old bars to flush

    for message in consumer:
        trade = message.value  # e.g. { 'symbol':..., 'timestamp':..., 'price':..., 'amount':... }
        update_bar(trade)

        # Periodically flush old bars
        now_ms = int(time.time() * 1000)
        if time.time() - last_flush_time > FLUSH_INTERVAL_SEC:
            flush_old_bars(influx_client, now_ms)
            last_flush_time = time.time()

if __name__ == "__main__":
    main()
