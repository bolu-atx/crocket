
class BittrexOrder:

    def __init__(self,
                 market=None,
                 order_type=None,
                 price=0,
                 target_quantity=0,
                 current_quantity=0,
                 execution_time=None,
                 completed=False):

        self.market = market
        self.type = order_type
        self.price = price
        self.target_quantity = target_quantity
        self.current_quantity = current_quantity
        self.execution_time = execution_time
        self.completed = completed

    @staticmethod
    def create(order):
        """
        Create a BittrexOrder from an order
        :param order:
        :return:
        """

        new_order = BittrexOrder(market=order.get('market'),
                                 order_type=order.get('type'),
                                 target_quantity=order.get('target_quantity'))

        return new_order
