import argparse, json, os, time
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaRecordDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import ProcessWindowFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

# Input tick format (JSON):
# {"ts": "2025-01-01T10:00:00.123Z", "ticker": "AAPL", "price": 195.12, "size": 100}

class MinuteBarFunction(ProcessWindowFunction):

    def process(self, key, context, elements, out):
        prices = [e[2] for e in elements]  # price
        sizes = [e[3] for e in elements]   # size
        if not prices:
            return
        open_p = prices[0]
        high_p = max(prices)
        low_p = min(prices)
        close_p = prices[-1]
        vol = sum(sizes)
        event_time = context.window().start  # epoch millis
        out.collect(json.dumps({
            "window_start": int(event_time),
            "ticker": key,
            "open": open_p, "high": high_p, "low": low_p, "close": close_p,
            "volume": vol
        }))

def to_tuple(value: str):
    obj = json.loads(value)
    # convert ts to epoch millis
    ts = int(datetime.fromisoformat(obj["ts"].replace("Z","+00:00")).timestamp() * 1000)
    return (ts, obj["ticker"], float(obj["price"]), int(obj["size"]))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default=os.getenv("KAFKA_BROKERS", "redpanda:9092"))
    parser.add_argument("--in-topic", default=os.getenv("KAFKA_TICKS_TOPIC", "ticks"))
    parser.add_argument("--out-topic", default=os.getenv("KAFKA_BARS_TOPIC", "minute_bars"))
    args = parser.parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))

    source = KafkaSource.builder() \
        .set_bootstrap_servers(args.brokers) \
        .set_group_id("flink-ticks") \
        .set_topics(args.in_topic) \
        .set_value_only_deserializer(KafkaRecordDeserializationSchema.value_only(SimpleStringSchema())) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(args.brokers) \
        .set_record_serializer(KafkaRecordSerializationSchema.builder() \
            .set_topic(args.out_topic) \
            .set_value_serialization_schema(SimpleStringSchema()) \
            .build()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "ticks-source")

    # Map JSON -> tuple(ts, ticker, price, size)
    parsed = ds.map(to_tuple, output_type=Types.TUPLE([Types.LONG(), Types.STRING(), Types.FLOAT(), Types.INT()]))

    # Assign watermarks on event time (ts field) with 5s out-of-orderness
    wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)).with_timestamp_assigner(
        lambda e, ts: e[0]
    )
    # Note: using no_watermarks() simplifies; if you want event-time, use the above and uncomment the next line:
    # parsed = parsed.assign_timestamps_and_watermarks(wm)

    # Window by ticker into 1-minute windows (processing-time here due to watermark comment above)
    from pyflink.datastream.window import TumblingProcessingTimeWindows
    windowed = parsed \
        .key_by(lambda e: e[1]) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1))) \
        .process(MinuteBarFunction(), output_type=Types.STRING())

    windowed.sink_to(sink)

    env.execute("ticks_to_minute_bars")

if __name__ == "__main__":
    from java.time import Duration  # pyflink exposes java.time.Duration
    main()
