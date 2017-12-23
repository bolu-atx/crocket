from enum import Enum
from decimal import Decimal

class OrderType(Enum):
    BUY = 1
    SELL = 2


class BittrexConstants:

    MARKET = 'market'
    DIGITS = Decimal('1e-8')