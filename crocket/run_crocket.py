from datetime import datetime, timedelta, timezone
from decimal import Decimal
from json import load as json_load
from logging import FileHandler, Formatter, StreamHandler, getLogger
from os import environ
from os.path import join
from sys import exit
from time import sleep

from requests.exceptions import ConnectionError

from bittrex.bittrex2 import Bittrex
from sql.sql import Database
from utilities.credentials import get_credentials


# ==============================================================================
# Functions
# ==============================================================================

def format_time(datetime_to_format, time_format="%Y-%m-%d %H:%M:%S.%f"):
    """
    Format datetime to string.
    Ex: 2017-09-22 12:28:22
    :return:
    """
    return datetime_to_format.strftime(time_format)


def convert_bittrex_timestamp_to_datetime(timestamp, time_format="%Y-%m-%dT%H:%M:%S.%f"):
    """
    Convert timestamp string to datetime.
    :param timestamp:
    :param time_format:
    :return:
    """
    try:
        converted_datetime = datetime.strptime(timestamp, time_format)
    except ValueError:
        converted_datetime = datetime.strptime('{}.0'.format(timestamp), time_format)

    return converted_datetime


def utc_to_local(utc_dt):
    """
    Convert UTC datetime to local datetime.
    :param utc_dt:
    :return:
    """
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def filter_bittrex_markets(markets, base_coin):
    """
    Filter all Bittrex markets using a base currency.
    :param markets: All bittrex markets
    :param base_coin: Base currency
    :return: (list)
    """
    return [x.get('MarketName') for x in markets
            if x.get('BaseCurrency') == base_coin and x.get('IsActive')]


def format_bittrex_entry(data, fields=('time', 'price', 'wprice', 'basevolume', 'buyorder', 'sellorder')):
    """
    Format data object (summary per interval) into SQL row format.
    :param data: Summary of market per interval
    :param fields: Keys to add in data
    :return: (list) tuples
    """

    return fields, [data.get(x) for x in fields]


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


def get_interval_index(entries, target_datetime, interval):
    """
    Get index of start and stop positions of interval from a list of data entries.
    :param entries: (list(dict))
    :param target_datetime: (datetime) Start of interval
    :param interval: (int) Seconds between data points
    :return:
    """
    timestamp_list = [convert_bittrex_timestamp_to_datetime(x.get('TimeStamp')) for x in entries]
    print(timestamp_list)
    print(target_datetime)

    stop_index = len([x for x in timestamp_list if x > target_datetime])
    start_index = len([x for x in timestamp_list if (x - target_datetime).total_seconds() > interval])

    return start_index, stop_index


# ==============================================================================
# Initialize logger
# ==============================================================================
logger = getLogger('crocket')

logger.setLevel(10)

fh = FileHandler(
    '/var/tmp/crocket.{:%Y:%m:%d:%H:%M:%S}.log'.format(datetime.now()))
fh.setFormatter(Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s'))
logger.addHandler(fh)

sh = StreamHandler()
sh.setFormatter(Formatter('%(levelname)s: %(message)s'))
logger.addHandler(sh)

logger.info('Initialized logger.')

# ==============================================================================
# Environment variables
# ==============================================================================

HOME_DIRECTORY_PATH = environ['HOME']
# CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials.json')
CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials_unlocked.json')

BITTREX_CREDENTIALS_PATH = join(HOME_DIRECTORY_PATH, 'bittrex_credentials.json')

HOSTNAME = 'localhost'
DATABASE_NAME = 'BITTREX2'

BASE_COIN = 'BTC'

# Data polling settings

sleep_time = 30  # seconds
interval = 60  # seconds

# ==============================================================================
# Run
# ==============================================================================

logger.debug('Starting CRocket ....................')

USERNAME, PASSCODE = get_credentials(CREDENTIALS_FILE_PATH)

# Load key/secret for bittrex API
with open(BITTREX_CREDENTIALS_PATH, 'r') as f:
    BITTREX_CREDENTIALS = json_load(f)

# Initialize database
db = Database(hostname=HOSTNAME,
              username=USERNAME,
              password=PASSCODE,
              database_name=DATABASE_NAME,
              logger=logger)

# Initialize Bittrex object
bittrex = Bittrex(api_key=BITTREX_CREDENTIALS.get('key'),
                  api_secret=BITTREX_CREDENTIALS.get('secret'),
                  api_version='v1.1')

# Get all markets on Bittrex
# bittrex_markets = bittrex.get_markets()
# MARKETS = filter_bittrex_markets(bittrex_markets, BASE_COIN)

MARKETS = ['BTC-CLUB']

# Create table for each market if doesn't exist
for market in MARKETS:
    db.create_price_table(market)

market = MARKETS[0]

failed_attempts = 0
metrics = None

initial_market_history = bittrex.get_market_history(market).get('result')

working_list = initial_market_history
current_datetime = convert_bittrex_timestamp_to_datetime(initial_market_history[0].get('TimeStamp'))

sleep(sleep_time)

try:

    while True:

        response = bittrex.get_market_history(market)

        if not response.get('success'):

            if failed_attempts >= 3:
                logger.debug('API request failed 3 times, exiting.')
                break

            failed_attempts += 1
            logger.debug('API query failed, attempting again in 30 seconds.')
            sleep(30)
            continue

        failed_attempts = 0

        market_history = response.get('result')
        last_id = working_list[0].get('Id')
        id_list = [x.get('Id') for x in market_history]

        if last_id in id_list:
            overlap_index = id_list.index(last_id)
            working_list = market_history[:overlap_index] + working_list

        else:
            working_list = market_history + working_list
            logger.debug('Latest ID in working list not found in latest market history. '
                         'Adding all latest market history to working list.')

        logger.debug('Working list size: {}'.format(str(len(working_list))))

        latest_datetime = convert_bittrex_timestamp_to_datetime(working_list[0].get('TimeStamp'))
        logger.debug('CURRENT DATETIME: {}'.format(format_time(current_datetime)))
        logger.debug('LATEST DATETIME: {}'.format(format_time(latest_datetime)))

        if (latest_datetime - current_datetime).total_seconds() > interval:

            start, stop = get_interval_index(working_list, current_datetime, interval)
            logger.debug('START: {}, STOP: {}'.format(str(start), str(stop)))

            if start == stop and (convert_bittrex_timestamp_to_datetime(
                                             working_list[start - 1].get('TimeStamp'))
                                              - current_datetime).total_seconds() <= interval:
                if metrics and len(metrics) > 0:
                    logger.debug('Generating metrics up until latest time.')
                    latest_time = convert_bittrex_timestamp_to_datetime(metrics[0].get('TimeStamp'))
                    while current_datetime < latest_time:
                        new_metrics = calculate_metrics(working_list[start:stop], current_datetime)

                        metrics['volume'] = new_metrics.get('volume')
                        metrics['buyorder'] = new_metrics.get('buyorder')
                        metrics['sellorder'] = new_metrics.get('sellorder')
                        metrics['time'] = new_metrics.get('time')

                        fields, row = format_bittrex_entry(metrics)
                        db.insert_query(market, fields, row)
                        current_datetime = current_datetime + timedelta(seconds=interval)
                        logger.debug('Entry added: {}'.format(';'.join(['{}: {}'.format(k, str(v))
                                                                        for k, v in formatted_entry])))
                else:
                    metrics = calculate_metrics(working_list[start:stop], current_datetime)

            else:

                if start == stop:
                    metrics = calculate_metrics([working_list[start - 1]], current_datetime)
                else:
                    metrics = calculate_metrics(working_list[start:stop], current_datetime)

                fields, row = format_bittrex_entry(metrics)
                db.insert_query(market, fields, row)
                current_datetime = current_datetime + timedelta(seconds=interval)
                logger.debug('Entry added: {}'.format(';'.join(['{}: {}'.format(k, str(v))
                                                                for k, v in formatted_entry])))

            working_list = working_list[:start]

        else:
            logger.debug('Difference between latest data point to last data point less than specified interval. '
                         'Skipping metrics generation.')

        sleep(sleep_time)

        # TODO: At midnight of every day - check and delete if any data past 30 days

except (KeyboardInterrupt, ConnectionError) as e:
    logger.debug('Error: {}. Exiting ...'.format(e))

db.close()
exit(0)
