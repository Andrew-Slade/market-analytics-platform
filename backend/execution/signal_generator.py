# W.I.P, templated from parquetizer

import json
import logging
from collections import defaultdict, deque
import os
from typing import Deque
import yaml
from datetime import datetime
import typing as tp
import pyarrow as pa
import pandas as pd
import deltalake
from pathlib import Path

import argparse
from confluent_kafka import Consumer, Message

__config_location: str = "/app/backend/config/logging.yml"
__log_location: str = "/app/backend/logs/parquetizer.log"
__kafka_conf: str = "/app/backend/config/kafka.yml"

class ParquetStreamer:
    def __init__(self, logger: logging.Logger, kafka_config: dict, date:str, reloadable: bool) -> None:
        self.logger: logging.Logger = logger
        self.kafka_config: dict = kafka_config["read"]
        self.kafka_config["group.id"] = f"{self.kafka_config.get("group.id", "parquetizer-group")}_{datetime.now().strftime("%Y%m%d%H%M%S")}"
        self.consumer = Consumer(self.kafka_config)
        self.polling_timeout = 1.0
        self.buffer_size = 100
        self.executedate = datetime.strptime(date, "%Y%m%d")
        self.reloadable = reloadable
        self.topic = f"market-data-{self.executedate.strftime("%Y%m%d")}"
        self.dlq = f"/app/backend/dead_letters/consumer_{datetime.now().strftime("%Y%m%d")}"
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
        self.error_count = 0
        self.total_count = 0
        self.logger.info(f"Initialized ParquetStreamer listening to topic {self.topic} using group id {self.kafka_config['group.id']} with buffer size {self.buffer_size} and polling timeout {self.polling_timeout}")

    def __enter__(self) -> tp.Any:
        return self
    
    def __exit__(self, exc_type: tp.Any, exc_value: tp.Any, traceback: tp.Any) -> None:
        self.logger.info("Spinning down ParquetStreamer")
        self.logger.info(f"Total messages processed: {self.total_count}")
        self.logger.info(f"Error messages: {self.error_count}")
        self.logger.info(f"Error percentage: {self.error_count / self.total_count * 100 if self.total_count > 0 else 0:.2f}%")
        self.consumer.close()
        for handler in self.logger.handlers:
            handler.flush()

    def stream_from_kafka(self) -> None:
        #TODO pickup from last left off
        self.parquet_tables: dict[str, str] = {}
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
                        self.handle_batch(message_queue, self.parquet_tables)
                        message_queue.clear()

    def create_parquet_stream(self, key: str) -> str:
        if self.reloadable:
            self.executedate = datetime.now()
        partition = f"/app/data/year={self.executedate.strftime('%Y')}/month={self.executedate.strftime('%m')}/day={self.executedate.strftime('%d')}"
        os.makedirs(partition, exist_ok=True)
        self.parquet_tables[key] = f"{partition}/{key}"
        return key

    def sparse_log_error(self, message: str, count = 0) -> int:
        count += 1
        if count >= 250:
            self.logger.error(message)
            count = 0
        return count

    def handle_batch(self, batch: Deque[tuple[str, dict]], parquet_tables: dict[str, str]) -> None:
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
            self.total_count += df.shape[0]
            try:
                table = pa.Table.from_pandas(df, schema=self.mschema)
            except Exception:
                df.to_csv(f"{self.dlq}_{key}_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}.dlq", index=False) #if we cant get the dataframe nicely into the pyarrow schema, dead letter queue it
                self.error_count += df.shape[0]
            else:
                try:
                    deltalake.write_deltalake(parquet_tables[key], table, mode="append")
                    self.logger.debug(f"Wrote batch of size {len(values)} to parquet for key {key}")
                except Exception as e:
                    self.logger.error(f"Error writing to parquet for key {key}: {e}")
                    continue
            
    def run(self) -> None:
        #stream from kafka, once messages hit the buffer size, write to parquet and clear buffer
        self.stream_from_kafka()


def setup_file_logger(name: str, logfile: str, verbose: bool = False) -> logging.Logger:
    Path(__log_location).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(__log_location),
            logging.StreamHandler()
        ],
        force=True
    )
    return logging.getLogger(name)


def arg_parser() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("-v", "--verbose", help="Enable debugging mode", action="store_true")
    p.add_argument("-d", "--date", default=None, help="Date in YYYYMMDD format")
    return p.parse_args()

if __name__ == "__main__":
    with open(__kafka_conf) as kfile:
        kconfig = yaml.safe_load(kfile)
    args = arg_parser()
    reloadable = False
    if args.date is None:
        args.date = datetime.now().strftime("%Y%m%d")
        reloadable = True
    logger: logging.Logger = setup_file_logger(
    "parquetizer",
    f"/app/backend/logs/parquetizer_{datetime.now():%Y-%m-%d}.log",
    verbose=args.verbose
    )
    with ParquetStreamer(logger=logger, kafka_config=kconfig, date=args.date, reloadable=reloadable) as ps:
        ps.run()