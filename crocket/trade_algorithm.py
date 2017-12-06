from numpy import mean, median
from decimal import Decimal

from utilities.time import convert_bittrex_timestamp_to_datetime, format_time, utc_to_local


def run_algorithm(market, data, status, wallet, buy_amount, bittrex, logger,
                  duration=3,
                  price_lag_time=30,
                  price_lag_duration=5,
                  price_lag_threshold=0.05,
                  volume_lag_duration=60,
                  volume_lag_threshold=25,
                  profit_percent=0.05,
                  stop_loss_percent=0.01,
                  stop_gain_percent=0.01,
                  max_hold_time=14400,
                  wait_time=14400,
                  digits=Decimal('1e-8')):

    time = data.get('datetime')
    buyvolume = data.get('buy_volume')
    wprice = data.get('wprice')

    current_time = time[-1]
    current_price = wprice[-1]

    last_buy_time_difference = (current_time - status.get('last_buy').get('start')).total_seconds()

    # Action if haven't bought coin
    if not status.get('bought'):

        # No action if purchased within time of last buy
        if last_buy_time_difference < wait_time:
            return status

        # No action if not enough to place buy order
        if wallet.get('BTC') < buy_amount:
            logger.info('Tradebot: Not enough in wallet to place buy order. Skipping.')
            status['last_buy'] = {'start': current_time,
                                  'buy_price': current_price}
            return status

        sample_volume_mean = mean(buyvolume[-duration:])
        volume_lag_total = sum(buyvolume[-(duration + volume_lag_duration):-duration])

        if sample_volume_mean > 0 and volume_lag_total < volume_lag_threshold:
            previous_price = Decimal(
                mean(wprice[-(duration + price_lag_time):-(duration + price_lag_time - price_lag_duration)])).quantize(digits)

            if sample_volume_mean > 2 and \
                    abs((current_price - previous_price) / previous_price) < price_lag_threshold and \
                    sum([1 if x > 1 else 0 for x in buyvolume[-duration:]]) >= 3:

                # TODO: get BTC in wallet available, if fails, continue - buy order will fail and bot resumes
                # TODO: get orderbook of market, if fails, set default buy rate value and continue with buy order
                buy_total = (buy_amount / current_price).quantize(digits)
                buy_rate = (current_price * Decimal(1.1)).quantize(digits)

                try:

                    buy_response = bittrex.buy_or_else(market, buy_total, buy_rate, logger=logger)

                    if buy_response.get('success'):
                        buy_result = buy_response.get('result')

                        status['bought'] = True
                        status['current_buy'] = {'start': utc_to_local(convert_bittrex_timestamp_to_datetime(buy_result.get('Closed'))),
                                                 'buy_price': Decimal(buy_result.get('PricePerUnit')).quantize(digits),
                                                 'buy_total': (Decimal(buy_result.get('Price')) +
                                                              Decimal(buy_result.get('CommissionPaid'))).quantize(digits),
                                                 'quantity': (Decimal(buy_result.get('Quantity')) -
                                                              Decimal(buy_result.get('QuantityRemaining'))).quantize(digits)}

                        wallet['BTC'] = (wallet.get('BTC') - status.get('current_buy').get('buy_total')).quantize(digits)
                        wallet[market] = (wallet.get(market) + status.get('current_buy').get('quantity')).quantize(digits)

                        logger.info('WALLET AMOUNT: {} BTC'.format(str(wallet.get('BTC'))))
                        logger.info('WALLET AMOUNT: {} {}'.format(str(wallet.get(market)), market.split('-')[-1]))
                    else:
                        # Buy order reached max API retry limit, tradebot continues
                        logger.info('Tradebot: Failed to buy {} @ {}'.format(market, current_time))

                except (ConnectionError, RuntimeError) as e:
                    # TODO: get open orders for specific market, and cancel open orders
                    # TODO: Send a message to user to manually sell
                    logger.info('Tradebot: Failed to buy {} @ {}: {}'.format(market, current_time, e))
                    logger.info('ACTION: Manually sell {}.'.format(market))
                finally:
                    status['last_buy'] = {'start': current_time,
                                          'buy_price': current_price}

    # Action if have bought coin
    else:
        current_buy = status.get('current_buy')
        current_buy_hold_time = (current_time - current_buy.get('start')).total_seconds()

        stop_gain_threshold = (current_buy.get('buy_price') * Decimal(stop_gain_percent + 1)).quantize(digits)

        # Activate stop gain signal after passing threshold percentage
        if current_price > stop_gain_threshold:
            status['stop_gain'] = True

        # Activate maximize gain signal after passing profit threshold
        if current_price > (current_buy.get('buy_price') * Decimal(profit_percent + 1)).quantize(digits):
            status['maximize_gain'] = True

        # Sell if hit stop loss
        # Sell after hitting profit threshold followed by drop in price of X%
        # Sell after passing max hold time
        # Sell after detecting stop gain signal and price drop below stop gain price
        if (current_price < (current_buy.get('buy_price') * Decimal(1 - stop_loss_percent)).quantize(digits)) or \
                status.get('maximize_gain') or \
                current_buy_hold_time > max_hold_time or \
                (status.get('stop_gain') and current_price < stop_gain_threshold):

            # TODO: Ensure sell order sells everything in one order
            sell_total = status.get('current_buy').get('quantity')
            sell_rate = (current_price * Decimal(0.8)).quantize(digits)

            try:

                sell_response = bittrex.sell_or_else(market, sell_total, sell_rate, logger=logger)

                if sell_response.get('success'):
                    sell_result = sell_response.get('result')

                    status['current_buy']['stop'] = utc_to_local(convert_bittrex_timestamp_to_datetime(sell_result.get('Closed')))

                    status['current_buy']['sell_price'] = Decimal(sell_result.get('PricePerUnit')).quantize(digits)

                    sell_total = (Decimal(sell_result.get('Price')) -
                                  Decimal(sell_result.get('CommissionPaid'))).quantize(digits)

                    status['current_buy']['sell_total'] = sell_total
                    status['current_buy']['profit'] = (sell_total - current_buy.get('buy_total')).quantize(digits)

                    wallet['BTC'] = (wallet.get('BTC') + sell_total).quantize(digits)
                    wallet[market] = (wallet.get(market) - current_buy.get('quantity')).quantize(digits)

                    logger.info('WALLET AMOUNT: {} BTC'.format(str(wallet.get('BTC'))))
                    logger.info('WALLET AMOUNT: {} {}'.format(str(wallet.get(market)), market.split('-')[-1]))

            except (ConnectionError, RuntimeError) as e:
                # TODO: Send a message to user to manually sell
                status['current_buy'] = {}  # Reset current buy if selling fails
                logger.info('Tradebot: Failed to sell {} @ {}: {}'.format(market, current_time, e))
                logger.info('ACTION: Manually sell {} of {}.'.format(str(current_buy.get('quantity')), market))
            finally:
                status['bought'] = False
                status['stop_gain'] = False
                status['maximize_gain'] = False

    return status, wallet
