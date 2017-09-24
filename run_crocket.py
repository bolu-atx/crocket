from datetime import datetime
from getpass import getpass
from os import environ
from os.path import join
from sys import exit
from time import sleep

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


def get_bittrex_markets(markets, base_coin):
    """
    Get all Bittrex markets using a base currency.
    :param markets: All bittrex markets
    :param base_coin: Base currency
    :return: (list)
    """
    return [x.get('MarketName') for x in markets.get('result')
            if x.get('BaseCurrency') == base_coin and x.get('IsActive')]


# ==============================================================================
# Environment variables
# ==============================================================================

HOME_DIRECTORY_PATH = environ['HOME']
CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials.json')

HOSTNAME = 'localhost'
DATABASE_NAME = 'PRICES'

BASE_COIN = 'BTC'

# ==============================================================================
# Run
# ==============================================================================

print('Starting CRocket ....................')

KEY = getpass('Enter decryption key: ')

cipher = AESCipher(KEY)

encrypted_username, encrypted_passcode = \
    map(str.encode, get_credentials(CREDENTIALS_FILE_PATH))

USERNAME = getpass('Enter username: ')

if cipher.decrypt(encrypted_username) != USERNAME:

    print('Username does not match encrypted username ...')
    exit(1)

PASSCODE = getpass('Enter passcode: ')

if cipher.decrypt(encrypted_passcode) != PASSCODE:

    print('Passcode does not match encrypted passcode ...')
    exit(1)

print('Successfully entered credentials ...')

# Initialize database
db = Database(hostname=HOSTNAME,
              username=USERNAME,
              password=PASSCODE,
              database_name=DATABASE_NAME)

# Initialize Bittrex object
bittrex = Bittrex()

# Get all markets on Bittrex
bittrex_markets = bittrex.get_markets()
MARKETS = get_bittrex_markets(bittrex_markets, BASE_COIN)

# Create table for each market if doesn't exist
for market in MARKETS:
    db.create_price_table(market)

try:

    while True:

        for market in MARKETS:

            bittrex_entry = bittrex.get_ticker(market)

            if not bittrex_entry.get('success'):

                print('Bittrex API call failed: {}'.format(bittrex_entry.get('message')))
                raise RuntimeError('API call failed')

            entry = (('time', get_time_now()), ('price', bittrex_entry.get('result').get('Last')))

            db.insert_query(market, entry)
            sleep(1)

        # TODO: At midnight of every day - check and delete if any data past 30 days

except (KeyboardInterrupt, RuntimeError) as e:
    print('Error: {}. Exiting ...'.format(e))

db.close()
exit(0)
