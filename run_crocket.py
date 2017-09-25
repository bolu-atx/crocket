from datetime import datetime
from getpass import getpass
from logging import FileHandler, Formatter, StreamHandler, getLogger
from requests.exceptions import ConnectionError
from os import environ
from os.path import join
from sys import exit
from time import sleep
from urllib3.exceptions import MaxRetryError, NewConnectionError

from utilities.passcode import AESCipher
from utilities.credentials import get_credentials
from bittrex.bittrex import Bittrex
from sql.sql import Database


# ==============================================================================
# Functions
# ==============================================================================

def get_time_now():
    """
    Get time now.
    Ex: 2017-09-22 12:28:22
    :return:
    """
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def filter_bittrex_markets(markets, base_coin):
    """
    Filter all Bittrex markets using a base currency.
    :param markets: All bittrex markets
    :param base_coin: Base currency
    :return: (list)
    """
    return [x.get('MarketName') for x in markets.get('result')
            if x.get('BaseCurrency') == base_coin and x.get('IsActive')]


def format_bittrex_entry(market_summary, time=get_time_now()):
    """
    Format market summary object into SQL entry.
    :param market_summary: Summary of market
    :param time: Time
    :return: (list) tuples
    """

    formatted_entry = []

    formatted_entry.append(('time', time))
    formatted_entry.append(('price', market_summary.get('Last')))
    formatted_entry.append(('basevolume', market_summary.get('BaseVolume')))
    formatted_entry.append(('volume', market_summary.get('Volume')))
    formatted_entry.append(('buyorder', market_summary.get('OpenBuyOrders')))
    formatted_entry.append(('sellorder', market_summary.get('OpenSellOrders')))

    return formatted_entry

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
CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials.json')

HOSTNAME = 'localhost'
DATABASE_NAME = 'BITTREX'

BASE_COIN = 'BTC'

API_MAX_RETRIES = 3

# ==============================================================================
# Run
# ==============================================================================

logger.debug('Starting CRocket ....................')

KEY = getpass('Enter decryption key: ')

cipher = AESCipher(KEY)

encrypted_username, encrypted_passcode = \
    map(str.encode, get_credentials(CREDENTIALS_FILE_PATH))

USERNAME = getpass('Enter username: ')

if cipher.decrypt(encrypted_username) != USERNAME:

    logger.debug('Username does not match encrypted username ...')
    exit(1)

PASSCODE = getpass('Enter passcode: ')

if cipher.decrypt(encrypted_passcode) != PASSCODE:

    logger.debug('Passcode does not match encrypted passcode ...')
    exit(1)

logger.debug('Successfully entered credentials ...')

# Initialize database
db = Database(hostname=HOSTNAME,
              username=USERNAME,
              password=PASSCODE,
              database_name=DATABASE_NAME,
              logger=logger)

# Initialize Bittrex object
bittrex = Bittrex()

# Get all markets on Bittrex
bittrex_markets = bittrex.get_markets()
MARKETS = filter_bittrex_markets(bittrex_markets, BASE_COIN)

# Create table for each market if doesn't exist
for market in MARKETS:
    db.create_price_table(market)

try:

    while True:

        try:

            market_summaries = bittrex.get_market_summaries()
            retries = 1

        except (ConnectionError, MaxRetryError, NewConnectionError) as e:

            # Retry API call on failed request
            logger.debug('Bittrex API call failed: {}'.format(e))

            if retries <= API_MAX_RETRIES:

                logger.debug('Retrying API call {}/{} in {} seconds ...'.format(retries, API_MAX_RETRIES, 60 * retries))
                sleep(60 * retries)

                retries += 1
                continue

            else:
                raise ConnectionError('Max number of consecutive API failed requests.')

        insert_time = get_time_now()

        for market_summary in market_summaries.get('result'):

            market = market_summary.get('MarketName')
            formatted_entry = format_bittrex_entry(market_summary, insert_time)
            db.insert_query(market, formatted_entry)

        sleep(60)

        # TODO: At midnight of every day - check and delete if any data past 30 days

except (KeyboardInterrupt, ConnectionError) as e:
    logger.debug('Error: {}. Exiting ...'.format(e))

db.close()
exit(0)
