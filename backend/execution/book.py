#W.I.P
from collections import deque, defaultdict
from backend.execution.utilities import SIDE
import logging

class Book:
    def __init__(self, logger: logging.Logger| None = None, ask_levels: dict[float, list[float]] | None = None, bid_levels: dict[float, list[float]] | None = None):
        self.asks: defaultdict[float, deque] = defaultdict(deque)
        self.bids: defaultdict[float, deque] = defaultdict(deque)
        self.ask_prices: set[float] = set()
        self.bid_prices: set[float] = set()
        self.logger: logging.Logger | None = logger if logger else None

        if ask_levels is not None:
            for price, sizes in ask_levels.items():
                for size in sizes:
                    self.asks[price].appendleft(size)
            self.ask_prices = set(sorted(list(self.asks.keys())))
        if bid_levels is not None:
            for price, sizes in bid_levels.items():
                for size in bid_levels[price]:
                    self.bids[price].appendleft(size)
            self.bid_prices = set(sorted(list(self.asks.keys())))


    def new_order(self, price, size, side: str):
        book = None
        try:
            oside = SIDE(side)
            if oside == SIDE.BUY:
                book = self.bids
            else:
                book = self.asks
        except Exception:
            if self.logger is not None:
                self.logger.error(f"Invalid order with side {side}")
            return False
        else:
            book[price].appendleft(size)
