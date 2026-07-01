import argparse
import logging
from datetime import datetime
import sys
import signal
import asyncio
import json
import typing as tp
import warnings
import websockets
import yaml
from pathlib import Path

from confluent_kafka import Producer, KafkaError, Message

warnings.filterwarnings("ignore", category=DeprecationWarning, module="yfinance")

__log_dir: str = "/app/backend/logs"
__kafka_conf: str = "/app/backend/config/kafka.yml"

class GenericIngestor:
    def __init__(self, symbol: str, logger: logging.Logger, kafka_conf: dict, url: str) -> None:
        self.symbol: str = symbol
        self.logger: logging.Logger  = logger
        self.url: str = url
        print(f"Kafka config: {kafka_conf["write"]}")
        self.kafka_conf: dict = kafka_conf["write"]
        self.producer = Producer(self.kafka_conf)
        self.logger.info(f"Initialized ingestor for {self.symbol} on {self.kafka_conf['bootstrap.servers']}")
        self.dlq = open(
            f"/app/backend/dead_letters/producer_{self.symbol}_{datetime.now():%Y%m%d}.dlq",
            "a+"
        )

    def __enter__(self) -> tp.Any:
        return self
    
    def __exit__(self, exc_type: tp.Any, exc_value: tp.Any, traceback: tp.Any) -> None:
        self.logger.info(f"Spinning down {self.symbol}")
        for handler in self.logger.handlers:
            handler.flush()
        try:
            self.dlq.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f"{GenericIngestor.__class__.__name__}_{self.symbol}"
    
    async def consume_api(self) -> None:
        backoff: int = 1
        subscription_message = json.dumps({
            "type": "subscribe",
            "product_ids": [self.symbol],
            "channels": [
                {
                    "name": "ticker",
                    "product_ids": [self.symbol]
                }
            ]
        })
        while True:
            self.logger.debug(f"Attempting to connect to {self.url} for {self.symbol}")
            try:
                async with websockets.connect(self.url, ping_interval=None) as websocket:
                    self.logger.debug(f"Connected to {self.url} for {self.symbol}")
                    await websocket.send(subscription_message)
                    while True:
                        self.logger.debug(f"Waiting for message for {self.symbol}")
                        response = await websocket.recv()
                        backoff = 1
                        response_dict: dict = json.loads(response)
                        if response_dict.get("type") == "ticker":
                            await self.__price_update_handler(response_dict)
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK, Exception) as e:
                self.logger.error(f"Connection lost for {self.symbol}: {str(e)}  Will Try again in {backoff} seconds")
                await asyncio.sleep(backoff)
                backoff = min (backoff * 2, 60)
                continue


    async def __price_update_handler(self, message: dict) -> None:
        self.logger.info(f"Message: {message}")
        message["Recieved"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
        try:
            self.producer.produce(f'market-data-{datetime.now().strftime("%Y%m%d")}', key=self.symbol, value=json.dumps(message).encode('utf-8'), callback=self.acked)
            self.producer.poll(0)
        except Exception:
            self.logger.warning(f"Message malformed for {self.symbol}")
            self.dlq.write(f"{message}\n")

    def acked(self, err: tp.Optional[KafkaError], msg: tp.Optional[Message]) -> None:
        if err is not None:
            self.logger.warning(f"Failed to deliver message: {str(msg)}: {str(err)}")
        else:
            self.logger.debug(f"Message produced: {str(msg)}")

def handle_sigterm(signum: tp.Optional[tp.Any], frame: tp.Optional[tp.Any]) -> None:
    sys.exit(0)

def arg_parser() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("ticker", type=str)
    p.add_argument("url", type=str)
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()

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

async def run_ingestor(args: argparse.Namespace, logger: logging.Logger, kconfig: dict) -> None:
        with GenericIngestor(args.ticker, logger, kconfig, args.url) as g:
            await g.consume_api()

if __name__=="__main__":
    args: argparse.Namespace = arg_parser()
    with open(__kafka_conf) as kfile:
        kconfig = yaml.safe_load(kfile)
    logger: logging.Logger = setup_file_logger(
        f"ingestor-{args.ticker}",
        f"/app/backend/logs/{args.ticker}_{datetime.now():%Y-%m-%d}.log",
        verbose=args.verbose
    )
    logger.info(f"Starting up {args.ticker}")
    try:
        signal.signal(signal.SIGTERM, handle_sigterm)
    except NotImplementedError:
        pass
    try:
        asyncio.run(run_ingestor(args, logger, kconfig))
    except SystemExit:
        pass #implemented by __exit__
    except Exception:
        logger.exception(f"Fatal error in {args.ticker}")
        sys.exit(1)