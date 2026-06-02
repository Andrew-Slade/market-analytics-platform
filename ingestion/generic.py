import yfinance #type: ignore[import-untyped]
import argparse
import logging
from datetime import datetime
import time
import sys
import signal

__log_dir = "logs"

class GenericIngestor:
    def __init__(self, symbol: str, logger: logging.Logger):
        self.symbol: str = symbol
        self.logger: logging.Logger  = logger

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.info(f"Spinning down {self.symbol}")
        for handler in self.logger.handlers:
            handler.flush()

    def __repr__(self):
        return f"{GenericIngestor.__class__.__name__}_{self.symbol}"
    
    def consume_api(self):
        try:
            with yfinance.WebSocket() as ws:
                ws.subscribe(self.symbol)
                time.sleep(2)
                ws.listen(self.__price_update_handler)
        except Exception as e:
            raise Exception(f"Invalid ticker {self.symbol}: {e}")

    def __price_update_handler(self, message):
        self.logger.debug(f"Message: {message}")
        #TODO write to kafka

def handle_sigterm(signum, frame):
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

if __name__=="__main__":
    args = arg_parser()
    logger = setup_logging(args.ticker, args.verbose)
    logger.info(f"Starting up {args.ticker}")
    try:
        signal.signal(signal.SIGTERM, handle_sigterm)
    except NotImplementedError:
        pass
    try:
        with GenericIngestor(args.ticker, logger) as g:
            g.consume_api()
    except SystemExit:
        pass #implemented by __exit__
    except Exception:
        logger.exception(f"Fatal error in {args.ticker}")
        sys.exit(1)