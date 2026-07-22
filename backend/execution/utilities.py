from enum import auto, StrEnum

class SIDE(StrEnum):
    SELL = auto()
    BUY = auto()
    SELL_SHORT = SELL