from enum import Enum
from decimal import Decimal


class OrderType(Enum):

    BUY = 1
    SELL = 2


class OrderStatus(Enum):

    UNEXECUTED = 1
    EXECUTED = 2
    COMPLETED = 3
    SKIPPED = 4


class BittrexConstants:

    MARKET = 'market'
    DIGITS = Decimal('1e-8')