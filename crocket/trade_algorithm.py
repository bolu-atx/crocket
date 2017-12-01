from numpy import mean, median


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
                  wait_time=14400):

    time = data.get('time')
    buyvolume = data.get('buy_volume')
    wprice = data.get('wprice')

    last_buy_time_difference = (time[-1] - status.get('last_buy').get('start')).total_seconds()

    # Action if haven't bought coin
    if not status.get('bought'):

        # No action if purchased within time of last buy
        if last_buy_time_difference < wait_time:
            return status

        sample_volume_mean = mean(buyvolume[-duration:])
        volume_lag_median = median(buyvolume[-(duration + volume_lag_duration):-duration])

        if sample_volume_mean > 0 and volume_lag_median < volume_lag_threshold:
            sample_price = float(wprice[-1])
            previous_price = float(
                mean(wprice[-(duration + price_lag_time):-(duration + price_lag_time - price_lag_duration)]))

            if sample_volume_mean > 2 and \
                            abs((sample_price - previous_price) / previous_price) < price_lag_threshold and \
                            sum([1 if x > 1 else 0 for x in buyvolume[-duration:]]) >= 3:
                # TODO: MAKE API CALL TO BUY (wrap in try)
                status['bought'] = True
                status['current_buy'] = {'start': time[-1],
                                         'buy_price': float(wprice[-1])}

    # Action if have bought coin
    else:
        current_buy = status.get('current_buy')
        current_buy_hold_time = (time[-1] - current_buy.get('start')).total_seconds()

        # Activate stop gain signal after passing threshold percentage
        if wprice[-1] > (current_buy.get('buy_price') * (stop_gain_percent + 1)):
            status['stop_gain'] = True

        # Activate maximize gain signal after passing profit threshold
        if wprice[-1] > (current_buy.get('buy_price') * (profit_percent + 1)):
            print(wprice[-1], current_buy.get('buy_price'), (current_buy.get('buy_price') * (profit_percent + 1)))
            status['maximize_gain'] = True

        # Sell if hit stop loss
        # Sell after hitting profit threshold followed by drop in price of X%
        # Sell after passing max hold time
        # Sell after detecting stop gain signal and price drop below stop gain price
        if (wprice[-1] < (current_buy.get('buy_price') * (1 - stop_loss_percent))) or \
                status.get('maximize_gain') or \
                current_buy_hold_time > max_hold_time or \
                (status.get('stop_gain') and wprice[-1] < (current_buy.get('buy_price') * (stop_gain_percent + 1))):
            # TODO: MAKE API CALL TO SELL (wrap in try)
            status['current_buy']['stop'] = time[-1]

            buy_price = current_buy.get('buy_price')
            sell_price = float(wprice[-1])

            status['current_buy']['sell_price'] = sell_price
            status['current_buy']['profit'] = ((sell_price - buy_price) / buy_price) * 0.9975 ** 2

            status['bought'] = False
            status['stop_gain'] = False
            status['maximize_gain'] = False

            status['last_buy'] = {'start': current_buy.get('start'),
                                  'buy_price': buy_price}

    return status
