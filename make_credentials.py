from argparse import ArgumentParser
from getpass import getpass
from sys import exit

from utilities.credentials import make_credentials
from utilities.passcode import AESCipher

# ==============================================================================
# Parse arguments
# ==============================================================================
parser = ArgumentParser()

parser.add_argument('-u',
                    '--username',
                    help='Username')

args = parser.parse_args()

# ==============================================================================
# Set up parameters
# ==============================================================================

USERNAME = args.username

PASSCODE = getpass('Enter pascode: ')

confirm_passcode = getpass('Confirm passcode: ')

if PASSCODE != confirm_passcode:

    print('Entered passcodes do not match.')
    exit(1)

KEY = getpass('Enter key to encrypt passcode: ')

confirm_key = getpass('Confirm key: ')

if KEY != confirm_key:

    print('Entered keys do not match.')
    exit(1)

cipher = AESCipher(KEY)

make_credentials(cipher.encrypt(USERNAME).decode(), cipher.encrypt(PASSCODE).decode())

print('Successfully created credentials.json ...')
