from utilities.constants import OrderStatus


def skip_order(order, order_list, out_queue):
    """
    Skip current order
    :param order: Current order
    :param order_list: Active orders
    :param out_queue: Queue of completed orders
    :return:
    """

    order.update_status(OrderStatus.SKIPPED.name)
    order_list.remove(order)
    out_queue.put(order)


