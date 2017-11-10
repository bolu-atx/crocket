from decimal import Decimal
from .time import format_time, utc_to_local


def get_interval_index(timestamp_list, target_datetime, interval):
    """
    Get index of start and stop positions of interval from a list of data entries.
    :param timestamp_list: list
    :param target_datetime: (datetime) Start of interval
    :param interval: (int) Seconds between data points
    :return:
    """

    stop_index = len([x for x in timestamp_list if x > target_datetime])
    start_index = len([x for x in timestamp_list if (x - target_datetime).total_seconds() > interval])

    return start_index, stop_index


def calculate_metrics(data, start_datetime, digits=8):
    """
    Calculate metrics.
    :param data: (list(dict)) Buy/sell orders over an interval
    :param start_datetime: Start of interval
    :param digits: (int) Number of decimal places
    :return:
    """
    decimal_places = Decimal(10) ** (digits * -1)

    volume = 0
    buy_order = 0
    sell_order = 0
    price = 0
    price_volume_weighted = 0
    formatted_time = format_time(utc_to_local(start_datetime),
                                 "%Y-%m-%d %H:%M:%S")

    if data and isinstance(data[0], dict):
        p, v, o = map(list, zip(*[(x.get('Price'), x.get('Total'), x.get('OrderType')) for x in data]))
        print(p, v, o)

        volume = Decimal(sum(v)).quantize(decimal_places)
        buy_order = sum([1 for x in o if x == 'BUY'])
        sell_order = len(o) - buy_order

        price = (sum([Decimal(x).quantize(decimal_places) for x in p]) / Decimal(len(p))).quantize(decimal_places)
        price_volume_weighted = (sum(
            [Decimal(x).quantize(decimal_places) * Decimal(y) for x, y in zip(p, v)]) / Decimal(sum(v))).quantize(
            decimal_places)

    metrics = {'basevolume': volume,
               'buyorder': buy_order,
               'sellorder': sell_order,
               'price': price,
               'wprice': price_volume_weighted,
               'time': formatted_time}

    return metrics
