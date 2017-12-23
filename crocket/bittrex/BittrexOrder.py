
class BittrexOrder:

    def __init__(self,
                 market=None,
                 order_type=None,
                 price=0,
                 target_quantity=0,
                 base_quantity=0,
                 current_quantity=0,
                 open_time=None,
                 closed_time=None,
                 completed=False,
                 uuid=None):

        self.market = market
        self.type = order_type
        self.price = price
        self.target_quantity = target_quantity
        self.base_quantity = base_quantity
        self.current_quantity = current_quantity
        self.open_time = open_time
        self.closed_time = closed_time
        self.completed = completed
        self.uuid = uuid

    @staticmethod
    def create(order):
        """
        Create a BittrexOrder from an order
        :param order:
        :return:
        """

        new_order = BittrexOrder(market=order.get('market'),
                                 order_type=order.get('type'),
                                 target_quantity=order.get('target_quantity'),
                                 base_quantity=order.get('base_quantity'))

        return new_order
