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

    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ==============================================================================
# Environment variables
# ==============================================================================

HOME_DIRECTORY_PATH = environ['HOME']
CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials.json')

HOSTNAME = 'localhost'
DATABASE_NAME = 'test'

MARKET = 'BTC-EDG'

# ==============================================================================
# Run
# ==============================================================================

print('Starting CRocket ....................')

KEY = getpass('Enter decryption key: ')

cipher = AESCipher(KEY)

encrypted_username, encrypted_passcode = map(str.encode, get_credentials(CREDENTIALS_FILE_PATH))

USERNAME = getpass('Enter username: ')

if cipher.decrypt(encrypted_username) != USERNAME:

    print('Username does not match encrypted username ...')
    exit(1)

PASSCODE = getpass('Enter pascode: ')

if cipher.decrypt(encrypted_passcode) != PASSCODE:

    print('Passcode does not match encrypted passcode ...')
    exit(1)

print('Successfully entered credentials ...')

# Initialize database
db = Database(hostname=HOSTNAME,
              username=USERNAME,
              password=PASSCODE,
              database=DATABASE_NAME)

# Initialize bittrex object
bittrex = Bittrex()

# Create table if it doesn't exist
db.create_coin_table(MARKET)

try:

    while True:

        bittrex_entry = bittrex.get_ticker(MARKET)

        if not bittrex_entry.get('success'):

            print('Bittrex API call failed: {}'.format(bittrex_entry.get('message')))
            break

        entry = (('time', get_time_now()), ('price', bittrex_entry.get('result').get('Last')))

        db.insert_query(MARKET, entry)
        sleep(60)

except KeyboardInterrupt:
    print('Keyboard exception received. Exiting ...')

db.close()
exit(0)
