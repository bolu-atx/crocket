from argparse import ArgumentParser
from getpass import getpass
from json import dump, load
from sys import exit

from .passcode import AESCipher


CREDENTIALS_FILE = 'credentials.json'


def make_credentials(username, passcode):

    with open(CREDENTIALS_FILE, 'w') as f:
        dump({'username': username, 'passcode': passcode}, f, indent=4)


def get_credentials(credentials_file_path):

    with open(credentials_file_path, 'r') as f:

        credentials = load(f)

        return credentials.get('username'), credentials.get('passcode')


if __name__ == '__main__':

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

    make_credentials(cipher.encrypt(USERNAME), cipher.encrypt(PASSCODE))





