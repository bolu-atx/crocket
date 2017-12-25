from decimal import Decimal

from utilities.constants import BittrexConstants, OrderStatus


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


def buy_above_bid(market, order, wallet, bittrex, logger,
                  percent=0.05):
    """
    Execute buy order above bid price
    :param market:
    :param order:
    :param wallet:
    :param bittrex:
    :param logger:
    :param percent:
    :return:
    """

    try:
        ticker = bittrex.get_ticker_or_else(market)

    except (ConnectionError, ValueError) as e:

        # Failed to get price - SKIP current order
        logger.debug('Manager: Failed to get price for {}: {}. Skipping buy order.'.format(
            e, market))
        # TODO: send telegram message
        return False

    bid_price = Decimal(str(ticker.get('Bid')))
    ask_price = Decimal(str(ticker.get('Ask')))

    price_buffer = ((ask_price - bid_price) * Decimal(str(percent)))

    buy_price = (bid_price + price_buffer).quantize(BittrexConstants.DIGITS)

    if buy_price > ask_price:
        buy_price = bid_price

    order.update_target_price(buy_price)

    # No action if not enough to place buy order - SKIP current order
    if wallet.get_quantity('BTC') < order.base_quantity:
        logger.info('Manager: Not enough in wallet to place buy order. Skipping {}.'.format(market))
        return False

    try:
        buy_response = bittrex.buy_limit(market, order.target_quantity, order.target_price)

        if buy_response.get('success'):
            order.update_uuid(buy_response.get('result').get('uuid'))
            order.update_status(OrderStatus.EXECUTED.name)
        else:
            logger.info('Manager: Failed to buy {}.'.format(market))
            raise ValueError('Manager: Execute buy order API call failed.')

    except (ConnectionError, ValueError) as e:

        # Failed to execute buy order - SKIP current order
        logger.debug(
            'Manager: Failed to execute buy order for {}: {}. Skipping buy order.'.format(
                market, e
            ))

        return False

    return True


def sell_below_ask(market, order, wallet, bittrex, logger,
                   percent=0.05):
    """
    Execute sell order below ask price
    Set percent too
    :param percent:
    :param market:
    :param order:
    :param active_orders:
    :param wallet:
    :param completed_queue:
    :param bittrex:
    :param logger:
    :return:
    """

    try:
        ticker = bittrex.get_ticker_or_else(market)

    except (ConnectionError, ValueError) as e:

        # Failed to get price - SKIP current order
        logger.error('Manager: Failed to get price for {}: {}. ACTION: Manually sell.'.format(
            e, market))
        # TODO: send telegram message
        return False

    bid_price = Decimal(str(ticker.get('Bid')))
    ask_price = Decimal(str(ticker.get('Ask')))

    price_buffer = ((ask_price - bid_price) * Decimal(str(percent)))

    sell_price = (ask_price - price_buffer).quantize(BittrexConstants.DIGITS)

    if sell_price < bid_price:
        sell_price = ask_price

    order.update_target_price(sell_price)

    # No action if nothing available for sell order - SKIP current order
    if wallet.get_quantity(market) == 0:
        logger.info('Manager: Not enough in wallet (0) to place sell order. Skipping {}.'.format(market))
        return False

    try:
        sell_response = bittrex.sell_limit(market, wallet.get_quantity(market), order.target_price)

        if sell_response.get('success'):
            order.update_uuid(sell_response.get('result').get('uuid'))
            order.update_status(OrderStatus.EXECUTED.name)
        else:
            logger.info('Manager: Failed to sell {}.'.format(market))
            raise ValueError('Manager: Execute sell order API call failed.')

    except (ConnectionError, ValueError) as e:

        # Failed to execute sell order - SKIP current order
        logger.debug(
            'Manager: Failed to execute sell order for {}: {}. Skipping sell order.'.format(
                market, e
            ))

        # TODO: send telegram message
        return False

    return True
