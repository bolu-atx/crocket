from numpy import array, convolve, dtype, exp, linspace
from decimal import Decimal

# ==============================================================================
# Calculate moving average
# ==============================================================================


def exp_moving_average_decimal(values, window):
    weights = array(exp(linspace(-1., 0., window)), dtype=dtype(Decimal))
    weights = [round(Decimal(x / weights.sum()), 8) for x in weights]
    a = convolve(values, weights, mode='full')[:len(values)]
    a[:window] = a[window]
    return a


def exp_moving_average(values, window):
    weights = exp(linspace(-1., 0., window))
    weights /= weights.sum()
    a = convolve(values, weights, mode='full')[:len(values)]
    a[:window] = a[window]
    return a


# ==============================================================================
# Calculate RSI
# ==============================================================================


def split_gain_loss(input_vector):
    gain = []
    loss = []

    for n in input_vector:

        if n >= 0:
            gain.append(n)

        else:
            loss.append(n * -1)

    return gain, loss


def calculate_difference(input_vector):
    return [f - i for i, f in zip(input_vector, input_vector[1:])]


def calculate_RSI(input_vector, num_periods, limit=0):
    diff = calculate_difference(input_vector[-limit:])

    average_gains = []
    average_losses = []

    rsi = []

    first_gain, first_loss = map(lambda x: sum(x) / num_periods, split_gain_loss(diff[:num_periods]))
    first_rsi = 100 - (100 / (1 + first_gain / first_loss))

    average_gains.append(first_gain)
    average_losses.append(first_loss)

    rsi.append(first_rsi)

    for index in range(num_periods, len(diff)):

        if diff[index] >= 0:
            current_gain = diff[index]
            current_loss = 0

        else:
            current_gain = 0
            current_loss = diff[index] * -1

        current_average_gain = (average_gains[-1] * (num_periods - 1) + current_gain) / num_periods
        current_average_loss = (average_losses[-1] * (num_periods - 1) + current_loss) / num_periods

        current_rsi = 100 - (100 / (1 + current_average_gain / current_average_loss))

        average_gains.append(current_average_gain)
        average_losses.append(current_average_loss)

        rsi.append(current_rsi)

    return rsi


def calculate_stoch_RSI(rsi_vector, num_periods):
    stoch_rsi = []

    for index in range(num_periods, len(rsi_vector)):
        rsi_min = min(rsi_vector[index - num_periods:index])
        rsi_max = max(rsi_vector[index - num_periods:index])

        current_stoch_rsi = ((rsi_vector[index - 1] - rsi_min) / (rsi_max - rsi_min))

        stoch_rsi.append(current_stoch_rsi)

    return stoch_rsi