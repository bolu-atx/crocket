from decimal import Decimal

from utilities.constants import BittrexConstants, OrderStatus


def skip_order(order, order_list, out_queue, logger):
    """
    Skip current order
    :param logger: Main logger
    :param order: Current order
    :param order_list: Active orders
    :param out_queue: Queue of completed orders
    :return:
    """

    order.status = OrderStatus.SKIPPED.name
    order_list.remove(order)
    out_queue.put(order)

    logger.info('SKIPPING {} order for {}.'.format(order.type, order.market))


def get_order_and_update_wallet(order, wallet, bittrex):
    """
    Get order data and update wallet with order data
    :param bittrex: Bittrex with credentials
    :param order: BittrexOrder
    :param wallet: Wallet
    :return:
    """

    order_response = bittrex.get_order(order.uuid)

    if order_response.get('success'):
        order_data = order_response.get('result')

        if order_data.get('QuantityRemaining') != order_data.get('Quantity'):

            order.upddate_order(order_response.get('result'))
            order.complete_order()

            if order_data.get('Type') == 'LIMIT_BUY':
                wallet.update_wallet(order.market, order.final_quantity, order.final_total)
            else:
                wallet.update_wallet(order.market, -1 * order.final_quantity, order.final_total)


def buy_above_bid(order, wallet, bittrex, logger,
                  percent=5):
    """
    Execute buy order above bid price
    :param order: BittrexOrder
    :param wallet: Wallet
    :param bittrex: Bittrex with credentials
    :param logger: Logger
    :param percent: Number between 0 and 100 to specify position in price differential
    :return:
    """

    try:
        ticker = bittrex.get_ticker_or_else(order.market)

    except (ConnectionError, ValueError) as e:

        # Failed to get price - SKIP current order
        logger.error('Manager: Failed to get price for {}: {}. Skipping buy order.'.format(
            e, order.market))
        # TODO: send telegram message
        return False

    bid_price = Decimal(str(ticker.get('Bid')))
    ask_price = Decimal(str(ticker.get('Ask')))

    price_buffer = ((ask_price - bid_price) * Decimal(str(percent / 100)))

    buy_price = (bid_price + price_buffer).quantize(BittrexConstants.DIGITS)

    if buy_price > ask_price:
        buy_price = bid_price

    order.target_price = buy_price

    # No action if not enough to place buy order - SKIP current order
    if wallet.get_quantity('BTC') < order.base_quantity:
        logger.info('Manager: Not enough in wallet to place buy order. Skipping {}.'.format(order.market))
        return False

    try:
        logger.info('BUYING: {}, QUANTITY: {}, PRICE: {}'.format(order.market, str(order.target_quantity),
                                                                 str(order.target_price)))

        buy_response = bittrex.buy_limit(order.market, order.target_quantity, order.target_price)

        if buy_response.get('success'):
            order.uuid = buy_response.get('result').get('uuid')
            order.status = OrderStatus.EXECUTED.name
            logger.info('Manager: Buy order for {} submitted successfully.'.format(order.market))
        else:
            logger.info('Manager: Failed to buy {}: {}.'.format(order.market, buy_response.get('message')))
            raise ValueError('Manager: Execute buy order API call failed: {}.'.format(buy_response.get('message')))

    except (ConnectionError, ValueError) as e:

        # Failed to execute buy order - SKIP current order
        logger.debug(
            'Manager: Failed to execute buy order for {}: {}. Skipping buy order.'.format(
                order.market, e
            ))

        return False

    return True


def sell_below_ask(order, wallet, bittrex, logger,
                   percent=5):
    """
    Execute sell order below ask price
    :param order: BittrexOrder
    :param wallet: Wallet
    :param bittrex: Bittrex with credentials
    :param logger: Logger
    :param percent: Number >= 0 to specify position in price differential
    :return:
    """

    try:
        ticker = bittrex.get_ticker_or_else(order.market)

    except (ConnectionError, ValueError) as e:

        # Failed to get price - SKIP current order
        logger.error('Manager: Failed to get price for {}: {}. ACTION: Manually sell.'.format(
            e, order.market))
        # TODO: send telegram message
        return False

    bid_price = Decimal(str(ticker.get('Bid')))
    ask_price = Decimal(str(ticker.get('Ask')))

    price_buffer = ((ask_price - bid_price) * Decimal(str(percent / 100)))

    sell_price = (ask_price - price_buffer).quantize(BittrexConstants.DIGITS)

    if sell_price < bid_price:
        sell_price = ask_price

    order.target_price = sell_price

    # No action if nothing available for sell order - SKIP current order
    if wallet.get_quantity(order.market) == 0:
        logger.info('Manager: Not enough in wallet (0) to place sell order. Skipping {}.'.format(order.market))
        return False

    try:
        logger.info('SELLING: {}, QUANTITY: {}, PRICE: {}'.format(order.market, str(order.target_quantity),
                                                                  str(order.target_price)))

        sell_response = bittrex.sell_limit(order.market, order.target_quantity, order.target_price)

        if sell_response.get('success'):
            order.uuid = sell_response.get('result').get('uuid')
            order.status = OrderStatus.EXECUTED.name
            logger.info('Manager: Sell order for {} submitted successfully.'.format(order.market))
        else:
            logger.info('Manager: Failed to sell {}: {}.'.format(order.market, sell_response.get('message')))
            raise ValueError('Manager: Execute sell order API call failed: {}.'.format(sell_response.get('message')))

    except (ConnectionError, ValueError) as e:

        # Failed to execute sell order - SKIP current order
        logger.error(
            'Manager: Failed to execute sell order for {}: {}. Skipping sell order.'.format(
                order.market, e
            ))

        # TODO: send telegram message
        return False

    return True
