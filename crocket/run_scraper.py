from concurrent.futures import as_completed
from datetime import datetime
from decimal import Decimal
from json import load as json_load
from logging import FileHandler, Formatter, StreamHandler, getLogger
from os import environ
from os.path import join
from random import randint, shuffle
from sys import exit
from time import sleep, time

from requests.exceptions import ConnectTimeout, ConnectionError, ProxyError, ReadTimeout
from requests_futures.sessions import FuturesSession

from bittrex.bittrex2 import Bittrex, filter_bittrex_markets, format_bittrex_entry, return_request_input
from sql.sql import Database
from utilities.credentials import get_credentials
from scraper_helper import process_data
from utilities.network import configure_ip, process_response

# ==============================================================================
# Initialize logger
# ==============================================================================
logger = getLogger('scraper')

logger.setLevel(10)

fh = FileHandler(
    '/var/tmp/scraper.{:%Y:%m:%d:%H:%M:%S}.log'.format(datetime.now()))
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

CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials_unlocked.json')

BITTREX_CREDENTIALS_PATH = join(HOME_DIRECTORY_PATH, 'bittrex_credentials.json')

PROXY_LIST_PATH = '/home/b3arjuden/crocket/proxy_list.txt'

MARKETS_LIST_PATH = '/home/b3arjuden/crocket/markets.txt'

HOSTNAME = 'localhost'
DATABASE_NAME = 'BITTREX2'

# Data polling settings

sleep_time = 15  # seconds
interval = 60  # seconds

# ==============================================================================
# Run
# ==============================================================================

logger.debug('Starting scraper ....................')

USERNAME, PASSCODE = get_credentials(CREDENTIALS_FILE_PATH)

# Load key/secret for bittrex API
with open(BITTREX_CREDENTIALS_PATH, 'r') as f:
    BITTREX_CREDENTIALS = json_load(f)

# Load proxies
with open(PROXY_LIST_PATH, 'r') as f:
    PROXIES = f.read().splitlines()

# Load markets
with open(MARKETS_LIST_PATH, 'r') as f:
    MARKETS = f.read().splitlines()

# Initialize database
db = Database(hostname=HOSTNAME,
              username=USERNAME,
              password=PASSCODE,
              database_name=DATABASE_NAME,
              logger=logger)

# Initialize Bittrex object
bittrex = Bittrex(api_key=BITTREX_CREDENTIALS.get('key'),
                  api_secret=BITTREX_CREDENTIALS.get('secret'),
                  dispatch=return_request_input,
                  api_version='v1.1')

# Create table for each market if doesn't exist
for market in MARKETS:
    db.create_price_table(market)

# Initialize variables
num_proxies = len(PROXIES)
randoms = list(range(num_proxies))

MAX_API_RETRY = 5

response_dict, working_data = {}, {}

futures = []

current_datetime = datetime.now().astimezone(tz=None)
current_datetime = {k: current_datetime for k in MARKETS}
last_price = {k: Decimal(0) for k in MARKETS}
weighted_price = {k: Decimal(0) for k in MARKETS}

try:

    with FuturesSession(max_workers=10) as session:

        while True:

            #print('Sending requests...')
            shuffle(randoms)
            start = time()

            for index in range(len(MARKETS)):

                market = MARKETS[index]
                request_input = bittrex.get_market_history(market)

                proxy = configure_ip(PROXIES[randoms[index]])
                url = request_input.get('url')
                headers = {"apisign": request_input.get('apisign')}

                response = session.get(url,
                                       background_callback=process_response,
                                       headers=headers,
                                       timeout=5,
                                       proxies=proxy)

                # Add attributes to response
                response.market = market
                response.url = request_input.get('url')
                response.headers = headers

                futures.append(response)

            for future in as_completed(futures):

                try:
                    response_dict[future.market] = future.result().data.get('result')
                except (ProxyError, ConnectTimeout, ConnectionError, ReadTimeout):
                    #logger.debug('Failed API call for {}.'.format(future.market))

                    api_retry = 0

                    while True:

                        #logger.debug('Retrying...')
                        r = randint(0, num_proxies - 1)
                        proxy = configure_ip(PROXIES[r])

                        try:
                            response = session.get(future.url,
                                                   background_callback=process_response,
                                                   headers=future.headers,
                                                   timeout=3,
                                                   proxies=proxy)
                            response_dict[future.market] = response.result().data.get('result')
                            break

                        except (ProxyError, ConnectTimeout, ConnectionError, ReadTimeout):
                            api_retry += 1
                            logger.debug('Retried API call failed for {}.'.format(future.market))

                            if api_retry >= MAX_API_RETRY:
                                logger.debug('MAX API RETRY LIMIT ({}) REACHED. SKIPPING {}.'.format(str(MAX_API_RETRY),
                                                                                              future.market))
                                break

                            pass

                    #logger.debug('Retried API call for {} successful.'.format(future.market))

            working_data, current_datetime, last_price, weighted_price, entries = \
                process_data(response_dict, working_data, current_datetime, last_price, weighted_price, logger, interval)

            if entries:
                db.insert_transaction_query(entries)
                #logger.debug('Inserted {} entries to database.'.format(str(len(entries))))

            stop = time()
            run_time = stop - start

            del futures[:]

            if run_time < sleep_time:
                sleep(sleep_time - run_time)

        # TODO: At midnight of every day - check and delete if any data past 30 days

except (KeyboardInterrupt, ConnectionError) as e:
    logger.debug('Error: {}. Exiting ...'.format(e))

db.close()
exit(0)
