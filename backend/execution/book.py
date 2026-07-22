from confluent_kafka import Consumer, Message
import yaml
from datetime import datetime
import typing as tp
import logging
import json
from dataclasses import dataclass
import argparse
import signal
from pathlib import Path
from collections import deque

@dataclass
class Book:
    bid: float
    bid_size: float
    ask: float
    ask_size: float

class SimulateL1Book:
    def __init__(self, symbol: str, config_path: str|None = None, shared_bbo_dict: tp.Any|None = None,  logger: logging.Logger| None = None):
        self.symbol = symbol
        self.running = True
        self.bbo = None
        self.logger = logger
        self.shared_bbo_dict = shared_bbo_dict

        self.kafka_config_path = config_path
        self.kafka_config: dict = self.read_kafka_config()["read"]
        self.kafka_config["group.id"] = f"{self.kafka_config.get('group.id', 'execution-group')}_{self.symbol}"
        self.topic = f"market-data-{self.symbol}-{datetime.now().strftime('%Y%m%d')}"
        self.consumer = Consumer(self.kafka_config)
        self.polling_timeout = 1
        self.last_ten_price_levels: deque[Book] = deque(maxlen=10)

    def read_kafka_config(self):
            with open(self.kafka_config_path) as kfile:
                return yaml.safe_load(kfile)

    def stream_L1_from_kafka(self) -> None:
        self.consumer.subscribe([self.topic])
        if self.logger:
            self.logger.info(f"Subscribed to topic {self.topic}, beginning to poll for messages...")
        try:
            while self.running:
                message: tp.Optional[Message] = self.consumer.poll(self.polling_timeout)
                if message is None:
                    continue
                elif message.error():
                    if self.logger:
                        self.logger.warning(f"Kafka error: {message.error()}")
                    continue
                else:
                    value = message.value()
                    key = message.key()
                    if key is None:
                        key = b''
                    if key is not None:
                        pkey = str(key.decode('utf-8'))
                        if pkey == self.symbol:
                            if value is not None:
                                nvalue = json.loads(value.decode('utf-8'))
                                self.update_bbo(float(nvalue["best_bid"]), float(nvalue["best_bid_size"]), float(nvalue["best_ask"]), float(nvalue["best_ask_size"]))
        finally:
            if self.consumer:
                self.consumer.close()

    def stop(self):
        self.running = False

    def update_bbo(self, bid= None, bid_size=None, ask = None, ask_size = None):
        if self.bbo is not None:
            self.last_ten_price_levels.appendleft(self.bbo)
        self.bbo = Book(bid=bid, bid_size=bid_size, ask=ask, ask_size=ask_size)
        if self.shared_bbo_dict is not None:
            self.shared_bbo_dict[self.symbol] = {
                "bid": bid,
                "bid_size": bid_size,
                "ask": ask,
                "ask_size": ask_size,
            }

def run_book_worker(symbol:str, kafka_conf:str, shared_bbo_dict: tp.Any|None = None, verbose: bool = False):
    """
    Used by multiprocessing
    """
    logger = setup_file_logger(
        f"books-{symbol}",
        f"/app/backend/logs/{symbol}_{datetime.now():%Y-%m-%d}.log",
        verbose=verbose,
    )
    book = SimulateL1Book(
        symbol = symbol,
        config_path=kafka_conf,
        shared_bbo_dict = shared_bbo_dict,
        logger = logger
    )
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, terminating book process gracefully...")
        book.stop()
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    book.stream_L1_from_kafka()


def setup_file_logger(name: str, logfile: str, verbose: bool = False) -> logging.Logger:
    Path(logfile).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler()
        ],
        force=True
    )

    return logging.getLogger(name)

def arg_parser() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("symbol", type=str)
    p.add_argument("config_path", type=str)
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()

if __name__=="__main__":
    args = arg_parser()
    run_book_worker(args.symbol, args.config_path, None, args.verbose)