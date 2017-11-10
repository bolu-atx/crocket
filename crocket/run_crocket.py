from datetime import datetime, timedelta
from json import load as json_load
from logging import FileHandler, Formatter, StreamHandler, getLogger
from os import environ
from os.path import join
from sys import exit
from time import sleep

from requests.exceptions import ConnectionError

from bittrex.bittrex2 import Bittrex, filter_bittrex_markets, format_bittrex_entry
from sql.sql import Database
from utilities.credentials import get_credentials
from utilities.metrics import calculate_metrics, get_interval_index
from utilities.time import format_time, convert_bittrex_timestamp_to_datetime

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

            timestamp_list = [convert_bittrex_timestamp_to_datetime(x.get('TimeStamp')) for x in working_list]
            start, stop = get_interval_index(timestamp_list, current_datetime, interval)
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
