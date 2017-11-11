from copy import deepcopy
from decimal import Decimal
from .time import format_time, utc_to_local
from datetime import timedelta

from ..bittrex.bittrex2 import convert_bittrex_timestamp_to_datetime, format_bittrex_entry


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


def process_data(input_data, working_data, market_datetime, last_price, interval=60):
    entries = []

    if not working_data:
        working_data = deepcopy(input_data)

    for market in working_data:

        working_list = working_data.get(market)
        input_list = input_data.get(market)
        current_datetime = market_datetime.get(market)

        last_id = working_list[0].get('Id')
        id_list = [x.get('Id') for x in input_list]

        if last_id in id_list:
            overlap_index = id_list.index(last_id)
            working_list = input_list[:overlap_index] + working_list
        else:
            working_list = input_list + working_list
            print('Latest ID in {} working list not found in input data. '
                  'Adding all input data to working list.')

        working_data[market] = working_list

        latest_datetime = utc_to_local(convert_bittrex_timestamp_to_datetime(working_list[0].get('TimeStamp')))

        if (latest_datetime - current_datetime).total_seconds() > interval:

            timestamp_list = [utc_to_local(convert_bittrex_timestamp_to_datetime(x.get('TimeStamp')))
                              for x in working_list]

            start, stop = get_interval_index(timestamp_list, current_datetime, interval)

            if start == stop:
                while (current_datetime + timedelta(seconds=interval)) < timestamp_list[start - 1]:
                    metrics = calculate_metrics(working_list[start:stop], current_datetime)

                    metrics['price'] = last_price.get(market)

                    fields, values = format_bittrex_entry(metrics)
                    entries.append((market, fields, values))

                    current_datetime = current_datetime + timedelta(seconds=interval)

                market_datetime[market] = current_datetime
            else:
                metrics = calculate_metrics(working_list[start:stop], current_datetime)

                fields, values = format_bittrex_entry(metrics)
                entries.append((market, fields, values))

                market_datetime[market] = current_datetime + timedelta(seconds=interval)
                last_price[market] = metrics.get('price')

            working_data[market] = working_list[:start]

    return working_data, market_datetime, last_price, entries
