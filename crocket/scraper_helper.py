from concurrent.futures import as_completed
from copy import deepcopy
from decimal import Decimal
from datetime import timedelta
from random import randint
from requests.exceptions import ConnectTimeout, ConnectionError, ProxyError, ReadTimeout

from bittrex.bittrex2 import format_bittrex_entry
from utilities.network import configure_ip, process_response
from utilities.time import convert_bittrex_timestamp_to_datetime, format_time, utc_to_local


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


def get_data(markets, bittrex, session, proxies, proxy_indexes, max_api_retry=3, logger=None):

    futures = []
    response_dict = {}
    num_proxies = len(proxies)

    for index in range(len(markets)):
        market = markets[index]
        request_input = bittrex.get_market_history(market)

        proxy = configure_ip(proxies[proxy_indexes[index]])
        url = request_input.get('url')
        headers = {"apisign": request_input.get('apisign')}

        response = session.get(url,
                               background_callback=process_response,
                               headers=headers,
                               timeout=3,
                               proxies=proxy)

        # Add attributes to response
        response.market = market
        response.url = request_input.get('url')
        response.headers = headers

        futures.append(response)

    for future in as_completed(futures):

        try:
            response_data = future.result().data

            if not response_data.get('success'):
                if response_data.get('message') == "INVALID_MARKET":
                    markets.remove(future.market)
                    logger.debug('Removed {}: invalid market ...'.format(future.market))
                continue

            response_dict[future.market] = response_data.get('result')
            if not response_dict[future.market]:
                if response_data.get('message') == "NO_API_RESPONSE":
                    raise ProxyError('NO API RESPONSE')

        except (ProxyError, ConnectTimeout, ConnectionError, ReadTimeout):

            api_retry = 0

            while True:

                if api_retry >= max_api_retry:
                    logger.debug('MAX API RETRY LIMIT ({}) REACHED. SKIPPING {}.'.format(str(max_api_retry),
                                                                                         future.market))
                    break

                r = randint(0, num_proxies - 1)
                proxy = configure_ip(proxies[r])

                try:
                    response = session.get(future.url,
                                           background_callback=process_response,
                                           headers=future.headers,
                                           timeout=2,
                                           proxies=proxy)
                    response_dict[future.market] = response.result().data.get('result')
                    if not response_dict[future.market]:
                        logger.debug('NO API RESPONSE, RETRYING: {} ...'.format(future.market))
                        api_retry += 1
                        continue

                    break

                except (ProxyError, ConnectTimeout, ConnectionError, ReadTimeout):
                    api_retry += 1
                    logger.debug('Retried API call failed for {}.'.format(future.market))

    return response_dict


def process_data(input_data, working_data, market_datetime, last_price, weighted_price, logger, interval=60):

    entries = []

    if not working_data:
        working_data = deepcopy(input_data)

    for market in working_data:

        input_list = input_data.get(market)
        working_list = working_data.get(market)

        try:
            last_id = working_list[0].get('Id')

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
            logger.debug('SKIPPED NUMBER OF ORDERS, HIGH ORDER VOLUME!!!!!!!!')
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
