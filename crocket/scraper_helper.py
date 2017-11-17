from copy import deepcopy
from decimal import Decimal
from utilities.time import convert_bittrex_timestamp_to_datetime, format_time, utc_to_local
from datetime import timedelta

from bittrex.bittrex2 import format_bittrex_entry


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
    buy_volume = 0
    sell_volume = 0
    buy_order = 0
    sell_order = 0
    price = 0
    price_volume_weighted = 0
    formatted_time = format_time(start_datetime,
                                 "%Y-%m-%d %H:%M:%S")

    if data and isinstance(data[0], dict):
        p, v, o = map(list, zip(*[(x.get('Price'), x.get('Total'), x.get('OrderType')) for x in data]))

        volume = Decimal(sum(v)).quantize(decimal_places)

        # Need this: volume can be 0
        # [{'Id': 20218449, 'TimeStamp': '2017-11-17T04:22:46.39',
        # 'Quantity': 1.5e-07, 'Price': 0.00021798, 'Total': 0.0,
        # 'FillType': 'PARTIAL_FILL', 'OrderType': 'BUY'}]
        if volume != 0:
            buy_volume = Decimal(sum([x for x, y in zip(v, o) if y == 'BUY'])).quantize(decimal_places)
            sell_volume = Decimal(sum([x for x, y in zip(v, o) if y == 'SELL'])).quantize(decimal_places)
            buy_order = sum([1 for x in o if x == 'BUY'])
            sell_order = len(o) - buy_order

            price = (sum([Decimal(x).quantize(decimal_places) for x in p]) / Decimal(len(p))).quantize(decimal_places)
            price_volume_weighted = (sum(
                [Decimal(x).quantize(decimal_places) * Decimal(y) for x, y in zip(p, v)]) / Decimal(sum(v))).quantize(
                decimal_places)

    metrics = {'basevolume': volume,
               'buyorder': buy_order,
               'sellorder': sell_order,
               'buyvolume': buy_volume,
               'sellvolume': sell_volume,
               'price': price,
               'wprice': price_volume_weighted,
               'time': formatted_time}

    return metrics


def process_data(input_data, working_data, market_datetime, last_price, weighted_price, logger, interval=60):
    entries = []

    if not working_data:
        working_data = deepcopy(input_data)

    for market in working_data:

        input_list = input_data.get(market)
        working_list = working_data.get(market)

        last_id = working_list[0].get('Id')

        try:
            if input_list[0].get('Id') < last_id:  # TODO: Why does this happen? current response has smaller ID than previous response
                continue
        # TypeError occurs when ID of latest response < latest ID of previous response
        # IndexError occurs when API call fails after max number of retries: input_list = []
        except (TypeError, IndexError):
            continue

        current_datetime = market_datetime.get(market)

        id_list = [x.get('Id') for x in input_list]

        if last_id in id_list:
            overlap_index = id_list.index(last_id)
            working_list = input_list[:overlap_index] + working_list
        else:
            working_list = input_list + working_list
            logger.debug('SKIPPED NUMBER OF ORDERS BECAUSE INTERVAL BETWEEN API CALLS TOO SHORT!!!!!!!!')
            logger.debug('Latest ID in {} working list not found in input data. Adding all input data to working list.'.format(market))

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
                    metrics['wprice'] = weighted_price.get(market)

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
                weighted_price[market] = metrics.get('wprice')

            working_data[market] = working_list[:start]

    return working_data, market_datetime, last_price, weighted_price, entries
