
class BittrexData:
    """
    Holds set of Bittrex data
    """

    def __init__(self,
                 market=None,
                 datetime=None,
                 wprice=None,
                 buy_volume=None,
                 sell_volume=None):

        self.market = market

        # Set properties to empty lists if not given a list
        if datetime is None:
            self.datetime = []
        elif isinstance(datetime, list):
            self.datetime = datetime
        else:
            raise TypeError('BittrexData: datetime argument is not a list.')

        if wprice is None:
            self.wprice = []
        elif isinstance(wprice, list):
            self.wprice = wprice
        else:
            raise TypeError('BittrexData: wprice argument is not a list.')

        if buy_volume is None:
            self.buy_volume = []
        elif isinstance(buy_volume, list):
            self.buy_volume = buy_volume
        else:
            raise TypeError('BittrexData: buy_volume argument is not a list.')

        if sell_volume is None:
            self.sell_volume = []
        elif isinstance(sell_volume, list):
            self.sell_volume = sell_volume
        else:
            raise TypeError('BittrexData: sell_volume argument is not a list.')

    def clear_first(self):
        """
        Clear the first entry in all data types
        :return:
        """

        del self.datetime[0]
        del self.wprice[0]
        del self.buy_volume[0]
        del self.sell_volume[0]

