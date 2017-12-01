from numpy import mean, median
from decimal import Decimal

def run_algorithm(data, status,
                  duration=3,
                  price_lag_time=30,
                  price_lag_duration=5,
                  price_lag_threshold=0.05,
                  volume_lag_duration=30,
                  volume_lag_threshold=2,
                  profit_percent=0.05,
                  stop_loss_percent=0.01,
                  stop_gain_percent=0.02,
                  max_hold_time=10800,
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

        sample_volume_mean = mean(buyvolume[-duration:])
        volume_lag_median = median(buyvolume[-(duration + volume_lag_duration):-duration])

        if sample_volume_mean > 0 and volume_lag_median < volume_lag_threshold:
            previous_price = Decimal(
                mean(wprice[-(duration + price_lag_time):-(duration + price_lag_time - price_lag_duration)])).quantize(digits)

            if sample_volume_mean > 2 and \
                    abs((current_price - previous_price) / previous_price) < price_lag_threshold and \
                    sum([1 if x > 1 else 0 for x in buyvolume[-duration:]]) >= 3:
                # TODO: MAKE API CALL TO BUY (wrap in try)
                status['bought'] = True
                status['current_buy'] = {'start': current_time,
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
            # TODO: MAKE API CALL TO SELL (wrap in try)
            status['current_buy']['stop'] = current_time

            buy_price = current_buy.get('buy_price')

            status['current_buy']['sell_price'] = current_price
            status['current_buy']['profit'] = (((current_price - buy_price) / buy_price) * Decimal(0.995)).quantize(digits)

            status['bought'] = False
            status['stop_gain'] = False
            status['maximize_gain'] = False

            status['last_buy'] = {'start': current_buy.get('start'),
                                  'buy_price': buy_price}

    return status
