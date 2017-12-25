from decimal import Decimal

ZERO = Decimal(0).quantize('1e-8')


class Wallet:
    """
    Tracks current balance of all markets
    """

    def __init__(self,
                 amount=ZERO,
                 markets=None):

        self._currencies = {
            'BTC': {
                'quantity': amount,
            }}

        if isinstance(markets, list):

            for market in markets:
                if isinstance(market, str):
                    self._currencies[market] = {
                        'quantity': ZERO,
                        'base_quantity': ZERO
                    }
                else:
                    raise TypeError('Wallet: markets must contain strings.')

        else:
            raise TypeError('Wallet: markets must be a list of market names.')

    def get_quantity(self, market):
        """
        Get quantity for market
        :param market:
        :return:
        """

        return self._currencies[market].get('quantity')

    def get_base_quantity(self, market):
        """
        Get quantity for market
        :param market:
        :return:
        """

        return self._currencies[market].get('base_quantity')

    def update_wallet(self, market, quantity, base_quantity):
        """
        Update wallet
        :param market:
        :param quantity: Quantity of market
        :param base_quantity: Quantity of market in BTC
        :return:
        """

        self._currencies['BTC']['quantity'] -= base_quantity
        self._currencies[market]['quantity'] += quantity
        self._currencies[market]['base_quantity'] += base_quantity

    def check_open_markets(self):
        """
        Check for any markets with non-zero quantity
        :return:
        """

        return {k: v for k, v in self._currencies.items() if k != 'BTC' and v.get('quantity') > 0}
