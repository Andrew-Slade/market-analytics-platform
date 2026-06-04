import logging
from collections import deque
from typing import Deque
import yaml
from datetime import datetime
import typing as tp
import collections

import argparse
from confluent_kafka import Consumer, Message

__config_location: str = "config/logging.yml"
__log_location: str = "logs/parquetizer.log"

class ParquetStreamer:
    def __init__(self, logger: logging.Logger, kafka_config: dict) -> None:
        self.logger: logging.Logger = logger
        self.kafka_config: dict = kafka_config["read"]
        self.kafka_config["group.id"] = f"{self.kafka_config.get("group.id", "parquetizer-group")}_{datetime.now().strftime("%Y%m%d%H%M%S")}"
        self.consumer = Consumer(self.kafka_config)
        self.polling_timeout = 1.0
        self.buffer_size = 10000
        self.topic = "market-data"
        self.logger.info(f"Initialized ParquetStreamer listening to topic {self.topic} using group id {self.kafka_config['group.id']} with buffer size {self.buffer_size} and polling timeout {self.polling_timeout}")
    
    def __enter__(self) -> tp.Any:
        return self
    
    def __exit__(self, exc_type: tp.Any, exc_value: tp.Any, traceback: tp.Any) -> None:
        self.logger.info("Spinning down ParquetStreamer")
        self.consumer.close()
        for handler in self.logger.handlers:
            handler.flush()

    def stream_from_kafka(self) -> None:
        message_queue: Deque = deque()
        self.consumer.subscribe([self.topic])
        self.logger.info(f"Subscribed to topic {self.topic}, beginning to poll for messages...")
        while True:
            message: tp.Optional[Message] = self.consumer.poll(self.polling_timeout)
            if message is None:
                self.sparse_log_error("No message received")
                continue
            elif message.error():
                self.sparse_log_error(f"Kafka error: {message.error()}")
                continue
            else:
                message_queue.append((message.key().decode('utf-8'), message.value().decode('utf-8')))
                if len(message_queue) > self.buffer_size:
                    self.handle_batch(message_queue)
                    message_queue.clear()

    def sparse_log_error(self, message: str, count = 0) -> None:
        count += 1
        if count >= 250:
            self.logger.error(message)
            count = 0
        return count

    def handle_batch(self, batch: Deque[tuple[str, str]]) -> None:
        self.logger.debug(f"Batch contents: {collections.Counter([message[0] for message in batch])}")
        # TODO write batch to parquet, partition by ticker, and date
        # additionally, add current timestamp to the message for latency metrics

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
    return p.parse_args()

if __name__ == "__main__":
    with open("config/kafka.yml") as kfile:
        kconfig = yaml.safe_load(kfile)
    args = arg_parser()
    logger = setup_logging(verbose=args.verbose)
    with ParquetStreamer(logger=logger, kafka_config=kconfig) as pq:
        pq.run()