import argparse
import logging
from datetime import datetime
import sys
import signal
import asyncio
import json
import typing as tp
import warnings

import yfinance #type: ignore[import-untyped]
from confluent_kafka import Producer, KafkaError, Message

warnings.filterwarnings("ignore", category=DeprecationWarning, module="yfinance")

__log_dir = "logs"

class GenericIngestor:
    def __init__(self, symbol: str, logger: logging.Logger) -> None:
        self.symbol: str = symbol
        self.logger: logging.Logger  = logger
        conf = {'bootstrap.servers': '127.0.0.1:9092'}
        self.producer = Producer(conf)
        self.logger.info(f"Initialized ingestor for {self.symbol} on {conf['bootstrap.servers']}")

    def __enter__(self) -> tp.Any:
        return self
    
    def __exit__(self, exc_type: tp.Any, exc_value: tp.Any, traceback: tp.Any) -> None:
        self.logger.info(f"Spinning down {self.symbol}")
        for handler in self.logger.handlers:
            handler.flush()

    def __repr__(self) -> str:
        return f"{GenericIngestor.__class__.__name__}_{self.symbol}"
    
    async def consume_api(self) -> None:
        try:
            async with yfinance.AsyncWebSocket() as ws:
                await ws.subscribe(self.symbol)
                await asyncio.sleep(2)
                await ws.listen(self.__price_update_handler)
        except Exception as e:
            raise Exception(f"Invalid ticker {self.symbol}: {e}")

    async def __price_update_handler(self, message: dict) -> None:
        self.logger.info(f"Message: {message}")
        message["Recieved"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
        self.producer.produce('market-data', key=self.symbol, value=json.dumps(message).encode('utf-8'), callback=self.acked)
        self.producer.poll(0)

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
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()

def setup_logging(ticker: str, verbose: bool) -> logging.Logger:
        log = f"{__log_dir}/{ticker}_{datetime.now().strftime("%Y-%m-%d")}.log"
        logger = logging.getLogger(__name__)
        logger.propagate = False
        file_handler = logging.FileHandler(log)
        logging.basicConfig(level=logging.INFO if not verbose else logging.DEBUG)
        logger.addHandler(file_handler)
        logger.info(f"Set logging destination: {log}, logging mode {logging.getLevelName(logger.getEffectiveLevel())}")
        return logger

async def run_ingestor(args: argparse.Namespace, logger: logging.Logger) -> None:
        with GenericIngestor(args.ticker, logger) as g:
            await g.consume_api()

if __name__=="__main__":
    args: argparse.Namespace = arg_parser()
    logger: logging.Logger = setup_logging(args.ticker, args.verbose)
    logger.info(f"Starting up {args.ticker}")
    try:
        signal.signal(signal.SIGTERM, handle_sigterm)
    except NotImplementedError:
        pass
    try:
        asyncio.run(run_ingestor(args, logger))
    except SystemExit:
        pass #implemented by __exit__
    except Exception:
        logger.exception(f"Fatal error in {args.ticker}")
        sys.exit(1)