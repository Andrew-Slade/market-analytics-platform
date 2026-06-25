import json
import logging
from collections import defaultdict, deque
import os
from typing import Deque
import yaml
from datetime import datetime
import typing as tp
from pyarrow.parquet import ParquetWriter
import pyarrow as pa
import pandas as pd

import argparse
from confluent_kafka import Consumer, Message

__config_location: str = "config/logging.yml"
__log_location: str = "logs/parquetizer.log"

class ParquetStreamer:
    def __init__(self, logger: logging.Logger, kafka_config: dict, date:str) -> None:
        self.logger: logging.Logger = logger
        self.kafka_config: dict = kafka_config["read"]
        self.kafka_config["group.id"] = f"{self.kafka_config.get("group.id", "parquetizer-group")}_{datetime.now().strftime("%Y%m%d%H%M%S")}"
        self.consumer = Consumer(self.kafka_config)
        self.polling_timeout = 1.0
        self.buffer_size = 100
        self.executedate = datetime.strptime(date, "%Y%m%d")
        self.topic = f"market-data-{self.executedate.strftime("%Y%m%d")}"
        self.mschema = pa.schema([
            ("type", pa.string()),
            ("sequence", pa.int64()),
            ("product_id", pa.string()),
            ("price", pa.float64()),
            ("open_24h", pa.float64()),
            ("volume_24h", pa.float64()),
            ("low_24h", pa.float64()),
            ("high_24h", pa.float64()),
            ("volume_30d", pa.float64()),
            ("best_bid", pa.float64()),
            ("best_bid_size", pa.float64()),
            ("best_ask", pa.float64()),
            ("best_ask_size", pa.float64()),
            ("last_size", pa.float64()),
            ("side", pa.string()),
            ("time", pa.timestamp("ns", tz="UTC")),
            ("trade_id", pa.int64()),
            ("Recieved", pa.timestamp("us")),
        ])
        self.numeric_cols = [
                'price', 'open_24h', 'volume_24h', 'low_24h', 'high_24h', 
                'volume_30d', 'best_bid', 'best_bid_size', 'best_ask', 
                'best_ask_size', 'last_size'
            ]
        self.logger.info(f"Initialized ParquetStreamer listening to topic {self.topic} using group id {self.kafka_config['group.id']} with buffer size {self.buffer_size} and polling timeout {self.polling_timeout}")

    def __enter__(self) -> tp.Any:
        return self
    
    def __exit__(self, exc_type: tp.Any, exc_value: tp.Any, traceback: tp.Any) -> None:
        self.logger.info("Spinning down ParquetStreamer")
        self.consumer.close()
        for handler in self.logger.handlers:
            handler.flush()
        if len(self.parquet_writers) > 0:
            for i in self.parquet_writers.values():
                i.close()

    def stream_from_kafka(self) -> None:
        #TODO pickup from last left off
        self.parquet_writers: dict[str, ParquetWriter] = {}
        message_queue: Deque = deque()
        self.consumer.subscribe([self.topic])
        self.logger.info(f"Subscribed to topic {self.topic}, beginning to poll for messages...")
        count: int = 0
        keys_seen: set[str] = set()
        while True:
            message: tp.Optional[Message] = self.consumer.poll(self.polling_timeout)
            if message is None:
                count = self.sparse_log_error("No message received", count)
                continue
            elif message.error():
                count = self.sparse_log_error(f"Kafka error: {message.error()}", count)
                continue
            else:
                value = message.value()
                key = message.key()
                if key is None:
                    key = b''
                if key is not None:
                    pkey = str(key.decode('utf-8'))
                    if value is not None:
                        nvalue = json.loads(value.decode('utf-8'))
                        nvalue["Consumed"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #mark when the consumer got this
                        message_queue.append((pkey, nvalue))
                        if pkey not in keys_seen:
                            keys_seen.add(self.create_parquet_stream(pkey))
                    if len(message_queue) > self.buffer_size :
                        self.handle_batch(message_queue, self.parquet_writers)
                        message_queue.clear()

    def create_parquet_stream(self, key: str) -> str:
        partition = f"data/year={self.executedate.strftime('%Y')}/month={self.executedate.strftime('%m')}/day={self.executedate.strftime('%d')}"
        os.makedirs(partition, exist_ok=True)
        self.parquet_writers[key] = ParquetWriter(f"{partition}/{key}.parquet", schema=self.mschema)
        return key

    def sparse_log_error(self, message: str, count = 0) -> int:
        count += 1
        if count >= 250:
            self.logger.error(message)
            count = 0
        return count

    def handle_batch(self, batch: Deque[tuple[str, dict]], parquet_writers: dict[str, ParquetWriter]) -> None:
        self.logger.debug(f"Buffer size exceeded: {len(batch)} messages in queue")
        batches = defaultdict(list)
        while len(batch) > 0:
            key, value = batch.popleft()
            batches[key].append(value)
        for key, values in batches.items():
            for i in values:
                i["Stored"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') #mark when the parquet wrote these
            df = pd.DataFrame(values) #create a dataframe with all the data from one symbol
            for col_name in self.mschema:
                if col_name.name not in df.columns:
                    df[col_name.name] = pd.NA #null out any empty columns
            df = df[self.mschema.names] #reduce columns
            for col in self.numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['time'] = pd.to_datetime(df['time'], utc=True).dt.tz_convert('UTC')
            df['Recieved'] = pd.to_datetime(df['Recieved'], format='%d-%m-%Y %H:%M:%S.%f').astype('datetime64[us]')
            table = pa.Table.from_pandas(df, schema=self.mschema)
            try:
                parquet_writers[key].write_table(table)
                self.logger.debug(f"Wrote batch of size {len(values)} to parquet for key {key}")
            except Exception as e:
                self.logger.error(f"Error writing to parquet for key {key}: {e}")
                continue
            
    def run(self) -> None:
        #stream from kafka, once messages hit the buffer size, write to parquet and clear buffer
        self.stream_from_kafka()


def setup_logging(verbose: bool) -> logging.Logger:
    try:
        print("Reading config")
        with open(__config_location) as lfile:
            log_conf = yaml.safe_load(lfile)
            print(f"Initialized config: {log_conf} from \n{__config_location}")

        logger = logging.getLogger(__name__)
        file_handler = logging.FileHandler(__log_location)
        logging.basicConfig(level=logging.INFO if not verbose else logging.DEBUG)
        logger.addHandler(file_handler)
        logger.info(f"Set logging destination: {__log_location}, logging mode {logging.getLevelName(logger.getEffectiveLevel())}")
    except Exception:
        print("Logging file format corrupted")
        exit(1)
    else:
        return logger

def arg_parser() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("-v", "--verbose", help="Enable debugging mode", action="store_true")
    p.add_argument("-d", "--date", default=datetime.now().strftime("%Y%m%d"), help="Date in YYYYMMDD format")
    return p.parse_args()

if __name__ == "__main__":
    with open("config/kafka.yml") as kfile:
        kconfig = yaml.safe_load(kfile)
    args = arg_parser()
    logger = setup_logging(verbose=args.verbose)
    with ParquetStreamer(logger=logger, kafka_config=kconfig, date=args.date) as ps:
        ps.run()