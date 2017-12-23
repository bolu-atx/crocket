from datetime import datetime
from decimal import Decimal

from bittrex.BittrexOrder import BittrexOrder
from utilities.constants import OrderType


class BittrexStatus:

    def __init__(self,
                 market=None,
                 bought=False,
                 stop_gain=False,
                 stop_gain_percent=0.02):

        self.market = market
        self.bought = bought
        self.stop_gain = stop_gain
        self.stop_gain_percent = stop_gain_percent

        self.last_buy_time = datetime(2017, 11, 11, 11, 11).astimezone(tz=None)
        self.last_buy_price = Decimal(0).quantize(Decimal(10) ** -8)

        self.buy_order = BittrexOrder(order_type=OrderType.BUY.name)
        self.sell_order = BittrexOrder(order_type=OrderType.SELL.name)

        self.buy_signal = None
        self.sell_signal = None

    def clear_orders(self):
        """
        Reset buy and sell order
        :return:
        """
        self.buy_order = BittrexOrder(order_type=OrderType.BUY.name)
        self.sell_order = BittrexOrder(order_type=OrderType.SELL.name)

