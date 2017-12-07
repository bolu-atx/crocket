from datetime import datetime
from decimal import Decimal
from flask import Flask, jsonify, request
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
from traceback import format_exc

from bittrex.bittrex2 import Bittrex, filter_bittrex_markets, format_bittrex_entry, return_request_input
from scraper_helper import get_data, process_data
from sql.sql import Database
from trade_algorithm import run_algorithm
from utilities.credentials import get_credentials
from utilities.time import format_time


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

DIGITS = Decimal(10) ** -8

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
# Tradebot settings
# ==============================================================================

WALLET_TOTAL = 0
AMOUNT_PER_CALL = 0

SKIP_LIST = []  # TODO: implement if necessary

MINIMUM_SELL_AMOUNT = Decimal(0.0006).quantize(DIGITS)

# ==============================================================================
# Set up queues and processes
# ==============================================================================

SCRAPER_QUEUE = Queue()
TRADEBOT_QUEUE = Queue()
SCRAPER_TRADEBOT_QUEUE = Queue()

scraper = Process()
tradebot = Process()

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
            ('buy_total', entry.get('buy_total')),
            ('sell_time', entry.get('stop')),
            ('sell_price', entry.get('sell_price')),
            ('sell_total', entry.get('sell_total')),
            ('profit', entry.get('profit')),
            ('percent', entry.get('percent'))]


def close_positions(bittrex, wallet, logger=None):

    for market in wallet:

        if market != 'BTC':
            try:
                sell_total = wallet.get(market)
                sell_rate = (MINIMUM_SELL_AMOUNT / sell_total).quantize(DIGITS)
                sell_response = bittrex.sell_or_else(market, wallet.get(market), sell_rate, logger)

                if sell_response.get('success'):

                    sell_result = sell_response.get('result')

                    sell_total = (Decimal(sell_result.get('Price')) -
                                  Decimal(sell_result.get('CommissionPaid'))).quantize(DIGITS)

                    wallet['BTC'] = (wallet.get('BTC') + sell_total).quantize(DIGITS)
                    logger.info('Tradebot: Successfully closed {}'.format(market))

            except (ConnectionError, RuntimeError) as e:
                if logger:
                    logger.error('Tradebot: Failed to close {}: {}.'.format(market, e))
                    logger.info('ACTION: Manually close {}.'.format(market))

    logger.info('FINAL WALLET AMOUNT: {}'.format(str(wallet.get('BTC'))))


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


# ==============================================================================
# Run functions
# ==============================================================================


def run_scraper(control_queue, database_name, logger, markets=MARKETS,
                max_api_retry=4, interval=60, sleep_time=10):

    # Initialize database object
    initialize_databases(database_name, markets, logger=logger)

    db = Database(hostname=HOSTNAME,
                  username=USERNAME,
                  password=PASSCODE,
                  database_name=database_name,
                  logger=logger)

    # Initialize Bittrex object
    bittrex_request = Bittrex(api_key=BITTREX_CREDENTIALS.get('key'),
                              api_secret=BITTREX_CREDENTIALS.get('secret'),
                              dispatch=return_request_input,
                              api_version='v1.1')

    # Initialize variables
    run_tradebot = False
    proxy_indexes = list(range(len(PROXIES)))

    working_data = {}

    current_datetime = datetime.now().astimezone(tz=None)
    current_datetime = {k: current_datetime for k in MARKETS}
    last_price = {k: Decimal(0).quantize(DIGITS) for k in MARKETS}
    weighted_price = {k: Decimal(0).quantize(DIGITS) for k in MARKETS}

    try:

        with FuturesSession(max_workers=20) as session:

            while True:
                shuffle(proxy_indexes)
                start = time()

                response_dict = get_data(MARKETS, bittrex_request, session, PROXIES, proxy_indexes,
                                         max_api_retry=max_api_retry, logger=logger)

                working_data, current_datetime, last_price, weighted_price, entries = \
                    process_data(response_dict, working_data, current_datetime, last_price, weighted_price, logger,
                                 interval)

                if run_tradebot:
                    tradebot_entries = {k: entries.get(k)[-1] for k in entries}

                    SCRAPER_TRADEBOT_QUEUE.put(tradebot_entries)

                if entries:
                    formatted_entries = list(chain.from_iterable(
                        [[(x, *format_bittrex_entry(y)) for y in entries[x]] for x in entries]))

                    db.insert_transaction_query(formatted_entries)

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


def run_tradebot(control_queue, data_queue, markets, wallet_total, amount_per_call, table_name, logger):

    data = {}
    status = {}
    wallet = {'BTC': wallet_total}

    bought_time = datetime(2017, 11, 11, 11, 11).astimezone(tz=None)

    last_buy = {'start': bought_time,
                'buy_price': 0}

    for market in markets:
        status[market] = {'bought': False,
                          'last_buy': last_buy,
                          'current_buy': {},
                          'stop_gain': False,
                          'maximize_gain': False}

        data[market] = {'datetime': [],
                        'wprice': [],
                        'buy_volume': []}

        wallet[market] = Decimal(0).quantize(DIGITS)

    db = Database(hostname=HOSTNAME,
                  username=USERNAME,
                  password=PASSCODE,
                  database_name=TRADEBOT_DATABASE,
                  logger=logger)

    # Initialize Bittrex object
    bittrex = Bittrex(api_key=BITTREX_CREDENTIALS.get('key'),
                      api_secret=BITTREX_CREDENTIALS.get('secret'),
                      api_version='v1.1')

    try:
        while True:

            scraper_data = data_queue.get()

            for market in scraper_data:

                if scraper_data.get(market).get('wprice') > 0:  # Temporary fix for entries with 0 price
                    data[market]['datetime'].append(scraper_data.get(market).get('datetime'))
                    data[market]['wprice'].append(scraper_data.get(market).get('wprice'))
                    data[market]['buy_volume'].append(scraper_data.get(market).get('buy_volume'))

                if len(data[market].get('datetime')) == 0:
                    print("ERROR", scraper_data.get(market))

            start = time()
            for market in scraper_data:

                if len(data.get(market).get('datetime')) > 90:

                    del data[market]['datetime'][0]
                    del data[market]['wprice'][0]
                    del data[market]['buy_volume'][0]

                    try:
                        status[market], wallet = run_algorithm(market,
                                                               data.get(market),
                                                               status.get(market),
                                                               wallet,
                                                               amount_per_call,
                                                               bittrex,
                                                               logger)
                    except Exception:
                        logger.error('Tradebot: ERROR during run algorithm.')
                        error_message = format_exc()
                        logger.error(error_message)

                        raise RuntimeError('Error from algorithm')

                    completed_buy = status.get(market).get('current_buy')

                    if completed_buy.get('profit'):
                        completed_buy['start'] = format_time(completed_buy['start'], "%Y-%m-%d %H:%M:%S")
                        completed_buy['stop'] = format_time(completed_buy['stop'], "%Y-%m-%d %H:%M:%S")

                        db.insert_query(table_name, format_tradebot_entry(market, completed_buy))
                        logger.info('Tradebot: completed order.', completed_buy)
                        status[market]['current_buy'] = {}

            stop = time()
            logger.info('Tradebot: Elapsed time: {}'.format(str(stop-start)))

            if not control_queue.empty():

                signal = control_queue.get()

                if signal == 'STOP':
                    logger.info('Tradebot: Stopping tradebot ...')
                    break

    except (ConnectionError, RuntimeError) as e:
        logger.error(e)
        logger.info('Tradebot: Stopping tradebot ...')
    finally:
        db.close()
        # TODO: check if any open orders

        if any(k for k in wallet if wallet.get(k) > 0 and k != 'BTC'):
            logger.info('Tradebot: Open positions remaining.')
            logger.info('Tradebot: Closing open positions.')
            close_positions(bittrex, wallet, logger)
        else:
            logger.info('FINAL WALLET AMOUNT: {}'.format(str(wallet.get('BTC'))))
            logger.info('Tradebot: No positions open. Safe to exit.')

        logger.info('Tradebot: Stopped tradebot.')
        logger.info('Tradebot: Database connection closed.')


# ==============================================================================
# Endpoints
# ==============================================================================


@app.route('/scraper/start/<database_name>', methods=['GET'])
def _scraper_start(database_name):

    main_logger.info('Detected SCRAPER: START endpoint.')
    main_logger.info('Starting scraper using database: {}.'.format(database_name))

    global scraper
    scraper = Process(target=run_scraper, args=(SCRAPER_QUEUE, database_name, main_logger))
    scraper.start()

    return jsonify("STARTED SCRAPER"), 200


@app.route('/scraper/stop', methods=['GET'])
def _scraper_stop():

    main_logger.info('Detected SCRAPER: STOP endpoint.')

    SCRAPER_QUEUE.put("STOP")

    scraper.join()

    return jsonify("STOPPED SCRAPER"), 200


@app.route('/tradebot/set/wallet/<float:amount>', methods=['GET'])
def _tradebot_set_wallet(amount):

    main_logger.info('Detected TRADEBOT: SET WALLET endpoint.')

    global WALLET_TOTAL
    WALLET_TOTAL = Decimal(amount).quantize(DIGITS)

    message = 'Tradebot: Wallet total set successfully: {}'.format(str(WALLET_TOTAL))
    main_logger.info(message)

    return jsonify(message), 200


@app.route('/tradebot/set/call/<float:amount>', methods=['GET'])
def _tradebot_set_call(amount):

    main_logger.info('Detected TRADEBOT: SET CALL endpoint.')

    global AMOUNT_PER_CALL
    AMOUNT_PER_CALL = Decimal(amount).quantize(DIGITS)

    message = 'Tradebot: Amount per call set successfully: {}'.format(str(AMOUNT_PER_CALL))
    main_logger.info(message)

    return jsonify(message), 200


@app.route('/tradebot/start/<table_name>', methods=['GET'])
def _tradebot_start(table_name):

    main_logger.info('Detected TRADEBOT: START endpoint.')

    SCRAPER_QUEUE.put('START TRADEBOT')

    if WALLET_TOTAL == 0:
        error = 'Tradebot: Error: Wallet total: {}. Must be > 0.'.format(str(WALLET_TOTAL))
        main_logger.error(error)

        return jsonify(error), 400

    if AMOUNT_PER_CALL < 0.0005:
        error = 'Tradebot: Error: Amount per call: {}. Must be > 0.0005.'.format(str(AMOUNT_PER_CALL))
        main_logger.error(error)

        return jsonify(error), 400

    global tradebot
    tradebot = Process(target=run_tradebot,
                       args=(TRADEBOT_QUEUE, SCRAPER_TRADEBOT_QUEUE, MARKETS,
                             WALLET_TOTAL, AMOUNT_PER_CALL, table_name, main_logger))
    tradebot.start()

    return jsonify('Tradebot: Started successfully. Wallet total: {}. Amount per call: {}'.format(
        str(WALLET_TOTAL), str(AMOUNT_PER_CALL))), 200


@app.route('/tradebot/stop', methods=['GET'])
def _tradebot_stop():

    main_logger.info('Detected TRADEBOT: STOP endpoint.')

    SCRAPER_QUEUE.put('STOP TRADEBOT')
    TRADEBOT_QUEUE.put("STOP")

    tradebot.join()

    return jsonify("STOPPED TRADEBOT"), 200


# TODO: Implement check that scraper and tradebot have successfully exited
@app.route('/shutdown', methods=['POST'])
def _shutdown():

    main_logger.info('Detected SHUTDOWN endpoint.')

    shutdown_server()

    return jsonify("SHUTTING DOWN SERVER"), 200


try:
    # ==========================================================================
    print('Starting server ...')
    # ==========================================================================
    app.run(debug=True, port=9999)
finally:

    # ==========================================================================
    print('Server terminated.')
    # ==========================================================================
