from getpass import getpass
from os import environ
from os.path import join
from sys import exit

from utilities.passcode import AESCipher
from utilities.credentials import get_credentials
from bittrex.bittrex import Bittrex
from sql.sql import Database


# ==============================================================================
# Environment variables
# ==============================================================================

HOME_DIRECTORY_PATH = environ['HOME']
CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials.json')

HOSTNAME = 'localhost'
DATABASE_NAME = 'test'

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

db = Database(hostname=HOSTNAME,
              username=USERNAME,
              password=PASSCODE,
              database=DATABASE_NAME)


bittrex = Bittrex()





