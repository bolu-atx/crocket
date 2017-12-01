from datetime import datetime
from decimal import Decimal
from flask import Flask, jsonify
from itertools import chain
from json import load as json_load
from requests.exceptions import ConnectionError
from requests_futures.sessions import FuturesSession
from logging import FileHandler, Formatter, StreamHandler, getLogger
from multiprocessing import Process, Queue
from os import environ
from os.path import dirname, join, realpath
from random import shuffle
from time import sleep, time

from bittrex.bittrex2 import Bittrex, filter_bittrex_markets, format_bittrex_entry, return_request_input
from scraper_helper import get_data, process_data
from sql.sql import Database
from trade_algorithm import run_algorithm
from utilities.credentials import get_credentials


# ==============================================================================
# Initialize logger
# ==============================================================================
main_logger = getLogger('scraper')

main_logger.setLevel(10)

fh = FileHandler(
    '/var/tmp/scraper.{:%Y:%m:%d:%H:%M:%S}.log'.format(datetime.now()))
fh.setFormatter(Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s'))
main_logger.addHandler(fh)

sh = StreamHandler()
sh.setFormatter(Formatter('%(levelname)s: %(message)s'))
main_logger.addHandler(sh)

main_logger.info('Initialized logger.')

# ==============================================================================
# Set up environment variables
# ==============================================================================

HOME_DIRECTORY_PATH = environ['HOME']

CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials_unlocked.json')

BITTREX_CREDENTIALS_PATH = join(HOME_DIRECTORY_PATH, 'bittrex_credentials.json')

CROCKET_DIRECTORY = dirname(dirname(realpath(__file__)))

PROXY_LIST_PATH = join(CROCKET_DIRECTORY, 'proxy_list.txt')

MARKETS_LIST_PATH = join(CROCKET_DIRECTORY, 'markets.txt')

# SQL parameters
HOSTNAME = 'localhost'

USERNAME, PASSCODE = get_credentials(CREDENTIALS_FILE_PATH)

TRADEBOT_DATABASE = 'TRADEBOT_RECORD'

app = Flask(__name__)


# ==============================================================================
# Load data
# ==============================================================================


# Load markets
with open(MARKETS_LIST_PATH, 'r') as f:
    MARKETS = f.read().splitlines()

# Load key/secret for bittrex API
with open(BITTREX_CREDENTIALS_PATH, 'r') as f:
    BITTREX_CREDENTIALS = json_load(f)

# Load proxies
with open(PROXY_LIST_PATH, 'r') as f:
    PROXIES = f.read().splitlines()


# ==============================================================================
# Set up queues
# ==============================================================================

SCRAPER_QUEUE = Queue()
TRADEBOT_QUEUE = Queue()
SCRAPER_TRADEBOT_QUEUE = Queue()

# ==============================================================================
# Helper functions
# ==============================================================================


def initialize_databases(database_name, markets, logger=None):

    db = Database(hostname=HOSTNAME,
                  username=USERNAME,
                  password=PASSCODE,
                  database_name='develop',
                  logger=logger)

    # Create database if does not exist
    db.create_database(database_name)
    db.create_database(TRADEBOT_DATABASE)

    db.close()

    base_db = Database(hostname=HOSTNAME,
                       username=USERNAME,
                       password=PASSCODE,
                       database_name=database_name,
                       logger=logger)

    # Create tables if does not exist
    for market in markets:
        base_db.create_price_table(market)

    base_db.close()

    tradebot_db = Database(hostname=HOSTNAME,
                           username=USERNAME,
                           password=PASSCODE,
                           database_name=TRADEBOT_DATABASE,
                           logger=logger)

    tradebot_db.create_trade_table(database_name)

    tradebot_db.close()


def format_tradebot_entry(market, entry):

    return [('market', market),
            ('buy_time', entry.get('start')),
            ('buy_price', entry.get('buy_price')),
            ('sell_time', entry.get('stop')),
            ('sell_price', entry.get('sell_price')),
            ('profit', entry.get('profit'))]

# ==============================================================================
# Run functions
# ==============================================================================


def run_scraper(control_queue, database_name, markets, logger,
                max_api_retry=4, interval=60, sleep_time=10):

    # Initialize database object
    initialize_databases(database_name, markets, logger=logger)

    db = Database(hostname=HOSTNAME,
                  username=USERNAME,
                  password=PASSCODE,
                  database_name=database_name,
                  logger=logger)

    # Initialize Bittrex object
    bittrex = Bittrex(api_key=BITTREX_CREDENTIALS.get('key'),
                      api_secret=BITTREX_CREDENTIALS.get('secret'),
                      dispatch=return_request_input,
                      api_version='v1.1')

    # Initialize variables
    run_tradebot = False
    proxy_indexes = list(range(len(PROXIES)))

    working_data = {}

    current_datetime = datetime.now().astimezone(tz=None)
    current_datetime = {k: current_datetime for k in MARKETS}
    last_price = {k: Decimal(0) for k in MARKETS}
    weighted_price = {k: Decimal(0) for k in MARKETS}

    try:

        with FuturesSession(max_workers=20) as session:

            while True:
                shuffle(proxy_indexes)
                start = time()

                response_dict = get_data(MARKETS, bittrex, session, PROXIES, proxy_indexes,
                                         max_api_retry=max_api_retry, logger=logger)

                working_data, current_datetime, last_price, weighted_price, entries = \
                    process_data(response_dict, working_data, current_datetime, last_price, weighted_price, logger,
                                 interval)

                if run_tradebot:
                    tradebot_entries = {k: entries.get(k)[-1] for k in entries}

                    SCRAPER_TRADEBOT_QUEUE.put(tradebot_entries)

                    logger.info("Scraper: Passing {} entries to tradebot.".format(str(len(tradebot_entries))))

                if entries:
                    formatted_entries = list(chain.from_iterable(
                        [[(x, *format_bittrex_entry(y)) for y in entries[x]] for x in entries]))

                    db.insert_transaction_query(formatted_entries)

                    logger.info("Scraper: Inserted {} entries into database".format(str(len(formatted_entries))))

                if not control_queue.empty():

                    signal = control_queue.get()

                    if signal == "START TRADEBOT":
                        run_tradebot = True
                        logger.info("Scraper: Starting tradebot ...")

                    elif signal == "STOP TRADEBOT":
                        run_tradebot = False
                        logger.info("Scraper: Stopping tradebot ...")

                    elif signal == "STOP":
                        logger.info("Scraper: Stopping scraper ...")
                        break

                stop = time()
                run_time = stop - start
                logger.info('Scraper: Elapsed time: {}'.format(str(run_time)))

                if run_time < sleep_time:
                    sleep(sleep_time - run_time)

    except ConnectionError as e:
        logger.debug('ConnectionError: {}. Exiting ...'.format(e))
    finally:
        db.close()

        logger.info("Scraper: Stopped scraper.")
        logger.info("Scraper: Database connection closed.")


def run_tradebot(control_queue, data_queue, markets, table_name, logger):

    data = {}
    status = {}

    bought_time = datetime(2017, 11, 16, 21, 59, 3)

    last_buy = {'start': bought_time,
                'buy_price': 0}

    for market in markets:
        status[market] = {'bought': False,
                          'last_buy': last_buy,
                          'current_buy': {},
                          'stop_gain': False,
                          'maximize_gain': False}

        data[market] = {'time': [],
                        'wprice': [],
                        'buy_volume': []}

    db = Database(hostname=HOSTNAME,
                  username=USERNAME,
                  password=PASSCODE,
                  database_name=TRADEBOT_DATABASE,
                  logger=logger)

    try:
        while True:

            scraper_data = data_queue.get()

            logger.info("TRADEBOT: Received {} entries from scraper.".format(str(len(scraper_data))))

            for market in scraper_data:

                if scraper_data.get(market).get('wprice') > 0:  # Temporary fix for entries with 0 price
                    data[market]['time'].append(scraper_data.get(market).get('time'))
                    data[market]['wprice'].append(scraper_data.get(market).get('wprice'))
                    data[market]['buy_volume'].append(scraper_data.get(market).get('buy_volume'))

            start = time()
            for market in scraper_data:

                if len(data.get(market).get('time')) > 60:

                    del data[market]['time'][0]
                    del data[market]['wprice'][0]
                    del data[market]['buy_volume'][0]

                    status[market] = run_algorithm(data.get(market), status.get(market))

                    completed_buy = status.get(market).get('current_buy')

                    if completed_buy.get('profit'):
                        db.insert_query(table_name, format_tradebot_entry(market, completed_buy))
                        logger.info("Tradebot: completed buy.", completed_buy)
                        status[market]['current_buy'] = {}

            stop = time()
            logger.info('Tradebot: Elapsed time: {}'.format(str(stop-start)))

            if not control_queue.empty():

                signal = control_queue.get()

                if signal == "STOP":
                    logger.info("Tradebot: Stopping tradebot ...")
                    break
    finally:
        db.close()
        # TODO: add functionality to sell all orders when exiting
        logger.info("Tradebot: Stopped tradebot.")
        logger.info("Tradebot: Database connection closed.")


# ==============================================================================
# Endpoints
# ==============================================================================


@app.route('/scraper/start/<database_name>', methods=['GET'])
def _scraper_start(database_name):

    print('Reached START SCRAPER endpoint.')
    print('Starting scraper using database: {}.'.format(database_name))

    scraper = Process(target=run_scraper, args=(SCRAPER_QUEUE, database_name, main_logger))
    scraper.start()

    return jsonify("STARTED SCRAPER"), 200


@app.route('/scraper/stop', methods=['GET'])
def _scraper_stop():

    print('Reached STOP SCRAPER endpoint.')

    SCRAPER_QUEUE.put("STOP")

    return jsonify("STOPPED SCRAPER"), 200


@app.route('/tradebot/start/<table_name>', methods=['GET'])
def _tradebot_start(table_name):

    print('Reached START TRADEBOT endpoint.')

    SCRAPER_QUEUE.put("START TRADEBOT")

    tradebot = Process(target=run_tradebot,
                       args=(TRADEBOT_QUEUE, SCRAPER_TRADEBOT_QUEUE, MARKETS, table_name, main_logger))
    tradebot.start()

    return jsonify("STARTED TRADEBOT"), 200


@app.route('/tradebot/stop', methods=['GET'])
def _tradebot_stop():

    print('Reached STOP TRADEBOT endpoint.')

    SCRAPER_QUEUE.put("STOP TRADEBOT")
    TRADEBOT_QUEUE.put("STOP")

    return jsonify("STOPPED TRADEBOT"), 200


# TODO: Implement method to shut down server
# @app.route('/shutdown', methods=['GET'])
# def _shutdown():
#
#     print('API call: shutdown.')
#
#     raise RuntimeError('Received shutdown signal.')


try:
    # ==========================================================================
    print('Starting server ...')
    # ==========================================================================
    app.run(debug=True, port=9999)
except RuntimeError:
    print('Shutting down server ...')
finally:

    # ==========================================================================
    print('Server terminated.')
    # ==========================================================================
