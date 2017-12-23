from numpy import mean
from decimal import Decimal

from utilities.time import convert_bittrex_timestamp_to_datetime, utc_to_local
from utilities.constants import OrderType


def run_algorithm(data, status, buy_amount, bittrex, order_queue, logger,
                  duration=1,
                  price_lag_time=30,
                  price_lag_duration=5,
                  price_lag_threshold=0.05,
                  volume_lag_duration=60,
                  buy_volume_lag_min=10,
                  buy_volume_lag_max=40,
                  sell_volume_lag_min=10,
                  sell_volume_lag_max=40,
                  stop_loss_percent=0.01,
                  stop_gain_increment=0.02,
                  max_hold_time=14400,
                  wait_time=14400,
                  digits=Decimal('1e-8')):

    market = status.market
    time = data.get('datetime')
    buyvolume = data.get('buy_volume')
    sellvolume = data.get('sell_volume')
    wprice = data.get('wprice')

    current_time = time[-1]
    current_price = wprice[-1]

    last_buy_time_difference = (current_time - status.last_buy_time).total_seconds()

    # Action if haven't bought coin
    if not status.bought:

        # No action if purchased within time of last buy
        if last_buy_time_difference < wait_time:
            return

        sample_buy_volume_mean = mean(buyvolume[-duration:])
        buy_volume_lag_total = sum(buyvolume[-(duration + volume_lag_duration):-duration])
        sell_volume_lag_total = sum(sellvolume[-(duration + volume_lag_duration):-duration])

        if sample_buy_volume_mean > 0 and \
            buy_volume_lag_min < buy_volume_lag_total < buy_volume_lag_max and \
                sell_volume_lag_min < sell_volume_lag_total < sell_volume_lag_max:

            previous_price = Decimal(
                mean(wprice[-(duration + price_lag_time):-(duration + price_lag_time - price_lag_duration)])).quantize(digits)

            if sample_buy_volume_mean > 2 and \
                    abs((current_price - previous_price) / previous_price) < price_lag_threshold:

                target_quantity = (buy_amount / current_price).quantize(digits)

                order_queue.put(
                    {
                        'market': market,
                        'type': OrderType.BUY.name,
                        'target_quantity': target_quantity,
                        'base_quantity': buy_amount
                    }
                )

                status.bought = True
                status.buy_signal = current_price

                status.last_buy_time = current_time
                status.last_buy_price = current_price

    # Action if have bought coin
    else:
        current_buy = status.get('current_buy')
        current_buy_hold_time = (current_time - current_buy.get('start')).total_seconds()

        if status.stop_gain_percent == 0.02:
            loss_threshold = 0
        else:
            loss_threshold = 0.01

        current_stop_gain_threshold = (
            current_buy.get('buy_price') * Decimal(status.stop_gain_percent + 1)).quantize(digits)
        current_stop_gain_min_threshold = (
            current_buy.get('buy_price') * Decimal(status.stop_gain_percent - loss_threshold + 1)).quantize(
            digits)

        next_stop_gain_threshold = (current_buy.get('buy_price') * Decimal(
            status.stop_gain_percent + stop_gain_increment + 1)).quantize(digits)

        # Activate stop gain signal after passing threshold percentage
        if not status.stop_gain and current_price > current_stop_gain_threshold:
            status.stop_gain = True
        elif status.stop_gain and current_price > next_stop_gain_threshold:
            status.stop_gain_percent = status.stop_gain_percent + stop_gain_increment

        # Sell if hit stop loss
        # Sell after passing max hold time
        # Sell after detecting stop gain signal and price drop below stop gain price
        if (current_price < (current_buy.get('buy_signal') * Decimal(1 - stop_loss_percent)).quantize(digits)) or \
                current_buy_hold_time > max_hold_time or \
                (status.stop_gain and current_price < current_stop_gain_min_threshold):

            # TODO: Ensure sell order sells everything in one order
            sell_total = status.get('current_buy').get('quantity')
            sell_rate = (current_price * Decimal(0.8)).quantize(digits)

            try:

                sell_response = bittrex.sell_or_else(market, sell_total, sell_rate, logger=logger)

                if sell_response.get('success'):
                    sell_result = sell_response.get('result')

                    status['current_buy']['stop'] = utc_to_local(convert_bittrex_timestamp_to_datetime(sell_result.get('Closed')))

                    status['current_buy']['sell_signal'] = current_price
                    status['current_buy']['sell_price'] = Decimal(sell_result.get('PricePerUnit')).quantize(digits)

                    sell_total = (Decimal(sell_result.get('Price')) -
                                  Decimal(sell_result.get('CommissionPaid'))).quantize(digits)
                    buy_total = current_buy.get('buy_total')
                    profit = (sell_total - buy_total).quantize(digits)

                    status['current_buy']['sell_total'] = sell_total
                    status['current_buy']['profit'] = profit
                    status['current_buy']['percent'] = (profit * Decimal(100) / buy_total).quantize(Decimal(10) ** -4)

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
                status.bought = False
                status.stop_gain = False
