
class BittrexOrder():

    def __init__(self,
                 market=None,
                 order_type=None,
                 target_quantity=0,
                 current_quantity=0,
                 execution_time=None):

        self.market = market
        self.type = order_type
        self.target_quantity = target_quantity
        self.current_quantity = current_quantity
        self.execution_time = execution_time

    @staticmethod
    def create(order):
        """
        Create a BittrexOrder from an order
        :param order:
        :return:
        """

        new_order = BittrexOrder(market=order.get('market'),
                                 order_type=order.get('order_type'),
                                 target_quantity=order.get('quantity'))

        return new_order
