from datetime import datetime

from bittrex.BittrexOrder import BittrexOrder
from utilities.constants import OrderType

DEFAULT_STOP_GAIN_PERCENT = 0.02


class BittrexStatus:

    def __init__(self,
                 market=None,
                 bought=False,
                 stop_gain=False,
                 stop_gain_percent=DEFAULT_STOP_GAIN_PERCENT):

        self.market = market
        self.bought = bought
        self.stop_gain = stop_gain
        self.stop_gain_percent = stop_gain_percent

        self.last_buy_time = datetime(2017, 11, 11, 11, 11).astimezone(tz=None)

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

    def set_buy_status(self, status):
        """
        Set buy status
        :param status: boolean
        :return:
        """

        assert type(status) is bool

        self.bought = status

    def set_stop_gain(self, status):
        """
        Set stop gain
        :param status:
        :return:
        """

        assert type(status) is bool

        self.stop_gain = status

    def reset_stop_gain(self):
        """
        Reset stop gain
        :return:
        """

        self.set_stop_gain(False)
        self.stop_gain_percent = DEFAULT_STOP_GAIN_PERCENT
