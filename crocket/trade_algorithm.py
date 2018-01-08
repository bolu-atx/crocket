from numpy import mean
from decimal import Decimal

from bittrex.BittrexStatus import DEFAULT_STOP_GAIN_PERCENT
from utilities.constants import BittrexConstants, OrderStatus, OrderType


def run_algorithm(data, status, buy_amount, order_queue, logger,
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
                  wait_time=14400):

    market = status.market
    time = data.datetime
    buyvolume = data.buy_volume
    sellvolume = data.sell_volume
    wprice = data.wprice

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
                mean(wprice[-(duration + price_lag_time):-(duration + price_lag_time - price_lag_duration)])).quantize(
                BittrexConstants.DIGITS)

            if sample_buy_volume_mean > 2 and \
                    abs((current_price - previous_price) / previous_price) < price_lag_threshold:

                target_quantity = (buy_amount / current_price).quantize(BittrexConstants.DIGITS)

                order = {
                    'market': market,
                    'type': OrderType.BUY.name,
                    'target_quantity': target_quantity,
                    'base_quantity': buy_amount
                }

                logger.info('BUY:\n{}'.format(order))

                order_queue.put(order)

                status.bought = True
                status.buy_signal = current_price
                status.last_buy_time = current_time

    # Action if have bought coin
    else:

        if status.buy_order.status != OrderStatus.COMPLETED.name:
            logger.error('Tradebot: Checking sell order when buy order still in progress: {}.'.format(market))
            return

        buy_order = status.buy_order
        current_buy_hold_time = (current_time - buy_order.closed_time).total_seconds()

        if status.stop_gain_percent == DEFAULT_STOP_GAIN_PERCENT:
            loss_threshold = 0
        else:
            loss_threshold = 0.01

        current_stop_gain_threshold = (
            status.buy_signal * Decimal(status.stop_gain_percent + 1)).quantize(BittrexConstants.DIGITS)
        current_stop_gain_min_threshold = (
            status.buy_signal * Decimal(status.stop_gain_percent - loss_threshold + 1)).quantize(
            BittrexConstants.DIGITS)

        next_stop_gain_threshold = (status.buy_signal * Decimal(
            status.stop_gain_percent + stop_gain_increment + 1)).quantize(BittrexConstants.DIGITS)

        # Activate stop gain signal after passing threshold percentage
        if not status.stop_gain and current_price > current_stop_gain_threshold:
            status.stop_gain = True
        elif status.stop_gain and current_price > next_stop_gain_threshold:
            status.stop_gain_percent = status.stop_gain_percent + stop_gain_increment

        # Sell if hit stop loss
        # Sell after passing max hold time
        # Sell after detecting stop gain signal and price drop below stop gain price
        if (current_price < (status.buy_signal * Decimal(1 - stop_loss_percent)).quantize(BittrexConstants.DIGITS)) or \
                current_buy_hold_time > max_hold_time or \
                (status.stop_gain and current_price < current_stop_gain_min_threshold):

            target_quantity = buy_order.final_quantity

            order = {
                'market': market,
                'type': OrderType.SELL.name,
                'target_quantity': target_quantity,
            }

            logger.info('SELL:\n{}'.format(order))

            order_queue.put(order)

            status.bought = False
            status.sell_signal = current_price
            status.reset_stop_gain()

