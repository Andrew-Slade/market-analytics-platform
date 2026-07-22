#W.I.P
from collections import defaultdict
from backend.execution.utilities import SIDE
import logging

class SimulatedL1Book:
    def __init__(self, logger: logging.Logger| None = None, ask_levels: dict[float, list[float]] | None = None, bid_levels: dict[float, list[float]] | None = None):
        self.asks: defaultdict[float, float] = defaultdict(float)
        self.bids: defaultdict[float, float] = defaultdict(float)
        self.ask_prices: set[float] = set()
        self.bid_prices: set[float] = set()
        self.logger: logging.Logger | None = logger if logger else None

        if ask_levels is not None:
            for price, sizes in ask_levels.items():
                for size in sizes:
                    self.asks[price] += size
            self.ask_prices = set(self.asks.keys())
        if bid_levels is not None:
            for price, sizes in bid_levels.items():
                for size in bid_levels[price]:
                    self.bids[price] += size
            self.bid_prices = set(self.asks.keys())


    def new_order(self, price: float, size: float, side: str):
        """
        Allow the addition of an order
        """
        book: defaultdict[float, float] | None = None
        levels: set | None = None
        try:
            oside = SIDE(side)
            if oside == SIDE.BUY:
                book,levels  = self.bids, self.bid_prices
            else:
                book, levels = self.asks, self.ask_prices
        except Exception:
            if self.logger is not None:
                self.logger.error(f"Invalid order with side {side}")
            return False
        book[price] += size
        levels.add(price)
        return True

    def match(self, price, size, side):
        pass

    def mock_l2(self, tick_price: float, tick_size: float, price_modifier: float, size_modifier: float):
        # self.new_order(tick_price + price_modifier, tick_size * size_modifier)
        pass