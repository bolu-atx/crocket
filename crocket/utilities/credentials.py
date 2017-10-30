from json import dump, load


CREDENTIALS_FILE = '.credentials.json'


def make_credentials(username, passcode):

    with open(CREDENTIALS_FILE, 'w') as f:
        dump({'username': username, 'passcode': passcode}, f, indent=4)


def get_credentials(credentials_file_path):

    with open(credentials_file_path, 'r') as f:

        credentials = load(f)

        return credentials.get('username'), credentials.get('passcode')




