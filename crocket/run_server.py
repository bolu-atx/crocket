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

from bittrex.bittrex2 import Bittrex, format_bittrex_entry, return_request_input
from bittrex.BittrexOrder import BittrexOrder
from bittrex.BittrexStatus import BittrexStatus
from bittrex.BittrexData import BittrexData
from scraper_helper import get_data, process_data
from sql.sql import Database
from trade_algorithm import run_algorithm
from utilities.constants import BittrexConstants, OrderStatus, OrderType
from utilities.credentials import get_credentials
from utilities.time import convert_bittrex_timestamp_to_datetime, format_time, utc_to_local
from utilities.Wallet import Wallet

# ==============================================================================
# Initialize logger
# ==============================================================================
main_logger = getLogger('scraper')

main_logger.setLevel(10)

fh = FileHandler(
    '/var/tmp/scraper.{:%Y:%m:%d:%H:%M:%S}.log'.format(datetime.now()))
fh.setFormatter(Formatter('%(asctime)s:%(levelname)s: %(message)s'))
main_logger.addHandler(fh)

sh = StreamHandler()
sh.setFormatter(Formatter('%(asctime)s:%(levelname)s: %(message)s'))
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
# Tradebot settings
# ==============================================================================

WALLET_TOTAL = 0
AMOUNT_PER_CALL = 0

SKIP_LIST = ['BTC-BCC', 'BTC-ETH', 'BTC-LSK', 'BTC-NEO', 'BTC-OMG', 'BTC-XRP', 'BTC-LTC']

MINIMUM_SELL_AMOUNT = Decimal(0.0006).quantize(BittrexConstants.DIGITS)

# ==============================================================================
# Set up queues and processes
# ==============================================================================

# Queues to control starting and ending processes
SCRAPER_QUEUE = Queue()
TRADEBOT_QUEUE = Queue()
MANAGER_QUEUE = Queue()

# Queues to pass data between processes
SCRAPER_TRADEBOT_QUEUE = Queue()
ORDER_QUEUE = Queue()
COMPLETED_ORDER_QUEUE = Queue()
# TRADEBOT_TELEGRAM_QUEUE = Queue()
# TELEGRAM_TRADEBOT_QUEUE = Queue()

# Initialization of global processes
scraper = Process()
tradebot = Process()
manager = Process()


# telegram = Process()

# ==============================================================================
# Helper functions
# ==============================================================================


def initialize_databases(database_name, markets, logger=None):
    """
    Create new database for data collection and new table for trades
    :param database_name:
    :param markets:
    :param logger:
    :return:
    """

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
    """
    Format trade entry for insertion into trade table
    :param market:
    :param entry:
    :return:
    """

    return [('market', market),
            ('buy_time', entry.get('start')),
            ('buy_signal', entry.get('buy_signal')),
            ('buy_price', entry.get('buy_price')),
            ('buy_total', entry.get('buy_total')),
            ('sell_time', entry.get('stop')),
            ('sell_signal', entry.get('sell_signal')),
            ('sell_price', entry.get('sell_price')),
            ('sell_total', entry.get('sell_total')),
            ('profit', entry.get('profit')),
            ('percent', entry.get('percent'))]


# TODO: refactor
def close_positions(bittrex, wallet, logger=None):
    """
    Close all open positions
    :param bittrex:
    :param wallet:
    :param logger:
    :return:
    """
    for market in wallet:

        sell_total = wallet.get(market)
        if market != 'BTC' and sell_total > 0:
            try:
                sell_rate = (MINIMUM_SELL_AMOUNT / sell_total).quantize(BittrexConstants.DIGITS)
                sell_response = bittrex.sell_or_else(market, wallet.get(market), sell_rate, logger=logger)

                if sell_response.get('success'):
                    sell_result = sell_response.get('result')

                    sell_total = (Decimal(sell_result.get('Price')) -
                                  Decimal(sell_result.get('CommissionPaid'))).quantize(BittrexConstants.DIGITS)

                    wallet['BTC'] = (wallet.get('BTC') + sell_total).quantize(BittrexConstants.DIGITS)
                    logger.info('Tradebot: Successfully closed {}'.format(market))

            except (ConnectionError, RuntimeError) as e:
                if logger:
                    logger.error('Tradebot: Failed to close {}: {}.'.format(market, e))
                    logger.info('ACTION: Manually close {}.'.format(market))

    logger.info('FINAL WALLET AMOUNT: {}'.format(str(wallet.get('BTC'))))


def skip_order(order, order_list, out_queue):
    """
    Skip current order
    :param order: Current order
    :param order_list: Active orders
    :param out_queue: Queue of completed orders
    :return:
    """

    order.update_status(OrderStatus.SKIPPED.name)
    order_list.remove(order)
    out_queue.put(order)


def shutdown_server():
    """
    Shutdown the server
    :return:
    """

    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


# ==============================================================================
# Run functions
# ==============================================================================

def run_scraper(control_queue, database_name, logger, markets=MARKETS,
                interval=60, sleep_time=5):
    """
    Run scraper to pull data from Bittrex
    :param control_queue:
    :param database_name:
    :param logger:
    :param markets:
    :param interval:
    :param sleep_time:
    :return:
    """
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
    last_price = {k: Decimal(0).quantize(BittrexConstants.DIGITS) for k in MARKETS}
    weighted_price = {k: Decimal(0).quantize(BittrexConstants.DIGITS) for k in MARKETS}

    try:

        with FuturesSession(max_workers=20) as session:

            while True:
                shuffle(proxy_indexes)
                start = time()

                response_dict = get_data(MARKETS, bittrex_request, session, PROXIES, proxy_indexes,
                                         logger=logger)

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
                logger.info('Tradebot: Total time: {}'.format(str(run_time)))

                if run_time < sleep_time:
                    sleep(sleep_time - run_time)

    except ConnectionError as e:
        logger.debug('ConnectionError: {}. Exiting ...'.format(e))
    finally:
        db.close()

        logger.info("Scraper: Stopped scraper.")
        logger.info("Scraper: Database connection closed.")


def run_tradebot(control_queue, data_queue, pending_order_queue, completed_order_queue,
                 markets, amount_per_call, table_name, logger, skip_list):
    """
    Run trading algorithm on real-time market data
    :param pending_order_queue: Queue to pass orders to manager
    :param completed_order_queue: Queue to recieve completed orders from manager
    :param control_queue: Queue to control tradebot
    :param data_queue: Queue to receive data from scraper
    :param markets: List of markets
    :param amount_per_call: Amount to purchase per buy order
    :param table_name: Name of SQL table to record data
    :param logger: Main logger
    :param skip_list: List of markets to skip
    :return:
    """

    market_data = {}
    market_status = {}
    completed_orders = []

    for market in markets:

        if market not in skip_list:
            market_status[market] = BittrexStatus(market=market)
            market_data[market] = BittrexData(market=market)

    # Initialize SQL database connection
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

            # Receive data from scraper
            scraper_data = data_queue.get()

            # Add received scraper data from running data
            for market, entry in scraper_data.items():

                if market not in skip_list:
                    if entry.get('wprice') > 0:  # Temporary fix for entries with 0 price
                        market_data[market].datetime.append(entry.get('datetime'))
                        market_data[market].wprice.append(entry.get('wprice'))
                        market_data[market].buy_volume.append(entry.get('buy_volume'))
                        market_data[market].sell_volume.append(entry.get('sell_volume'))

            # Check if any orders completed
            if not completed_order_queue.empty():

                while not completed_order_queue.empty():
                    completed_order = BittrexOrder.create(completed_order_queue.get())

                    completed_orders.append(completed_order)

                # Update market statuses with completed orders
                for order in completed_orders:

                    order_market = order.market
                    if order.type == OrderType.BUY.name:
                        market_status[order_market].buy_order = order
                    else:
                        market_status[order_market].sell_order = order

                completed_orders.clear()

            for market in scraper_data.keys():

                if market not in skip_list:
                    if len(market_data.get(market).datetime) > 65:

                        # Clear the first entries
                        market_data[market].clear_first()

                        status = market_status.get(market)

                        # TODO: format orders and send to order manager
                        run_algorithm(market_data.get(market),
                                      status,
                                      amount_per_call,
                                      bittrex,
                                      pending_order_queue,
                                      logger)

                        # Completed buy and sell order for single market
                        # TODO: Update completed_buy formatting
                        if status.buy_order.completed and status.sell_order.completed:
                            completed_buy['start'] = format_time(completed_buy.get('start'), "%Y-%m-%d %H:%M:%S")
                            completed_buy['stop'] = format_time(completed_buy.get('stop'), "%Y-%m-%d %H:%M:%S")

                            logger.info('Tradebot: completed buy/sell order for {}.'.format(market), completed_buy)

                            db.insert_query(table_name, format_tradebot_entry(market, completed_buy))

                            # Reset buy and sell orders
                            status.clear_orders()

            if not control_queue.empty():

                signal = control_queue.get()

                if signal == 'STOP':
                    logger.info('Tradebot: Stopping tradebot ...')
                    break

    except (ConnectionError, ValueError) as e:
        logger.error(e)
        logger.info('Tradebot: Stopping tradebot ...')
    finally:
        db.close()

        logger.info('Tradebot: Stopped tradebot.')
        logger.info('Tradebot: Database connection closed.')


def run_manager(order_queue, completed_queue, wallet_total, logger,
                sleep_time=5):
    """
    Handles execution of all orders
    :param order_queue: Queue receiving orders
    :param completed_queue: Queue sending completed orders
    :param wallet_total: Amount in wallet available
    :param logger: Main logger
    :param sleep_time: Duration between polling
    :return:
    """

    new_orders = []
    active_orders = []

    # Initialize Wallet
    wallet = Wallet(amount=wallet_total)

    # Initialize Bittrex object
    bittrex = Bittrex(api_key=BITTREX_CREDENTIALS.get('key'),
                      api_secret=BITTREX_CREDENTIALS.get('secret'),
                      api_version='v1.1')

    try:
        while True:

            # Check if any orders to be executed
            if not order_queue.empty():

                while not order_queue.empty():
                    new_order = BittrexOrder.create(order_queue.get())

                    new_orders.append(new_order)

            start = time()

            active_orders += new_orders

            for order in active_orders:

                market = order.get('market')

                # Actions for buy order
                if order.type == OrderType.BUY.name:

                    # Check status of buy order if executed
                    if order.status == OrderStatus.EXECUTED.name:
                        try:
                            order_response = bittrex.get_order(order.uuid)

                            if order_response.get('success'):
                                order_data = order_response.get('result')

                                # First time getting order data
                                if order.open_time is not None:
                                    order.open_time = utc_to_local(convert_bittrex_timestamp_to_datetime(
                                        order_data.get('Opened')))

                                # Check if buy order has executed or passed open duration
                                if not order_data.get('IsOpen') or \
                                        (datetime.now().astimezone(tz=None) - order.open_time).total_seconds() > 60:

                                    # Buy order is not complete - cancel order
                                    if order.current_quantity < order.target_quantity:
                                        cancel_response = bittrex.cancel(order.uuid)

                                        if not cancel_response.get('success'):
                                            # TODO: add telegram message here
                                            logger.error('Manager: Failed to cancel buy order for {}.'.format(market))
                                            logger.error('Manager: May need to manually cancel order.')

                                    order.add_completed_order(order_data)
                                    wallet.update_wallet(market, order.current_quantity, order.cost)

                                    logger.info('WALLET AMOUNT: {} BTC'.format(str(wallet.get_quantity('BTC'))))
                                    logger.info('WALLET AMOUNT: {} {}'.format(str(wallet.get_base_quantity(market)),
                                                                              market.split('-')[-1]))

                                    active_orders.remove(order)
                                    completed_queue.put(order)

                            else:
                                raise ValueError('Manager: Get buy order data API call failed.')

                        except (ConnectionError, ValueError) as e:

                            # Failed to get buy order data - SKIP current order
                            logger.debug('Manager: Failed to get buy order data for {}: {}.'.format(market, e))

                            cancel_response = bittrex.cancel(order.uuid)

                            if not cancel_response.get('success'):
                                # TODO: add telegram message here
                                logger.error('Manager: Failed to cancel buy order for {}.'.format(market))
                                logger.error('Manager: May need to manually cancel order.')

                            skip_order(order, active_orders, completed_queue)

                    # Buy order has not been executed
                    else:

                        try:
                            ticker = bittrex.get_ticker(market)

                            if not ticker.get('success'):
                                raise ValueError('Manager: Get ticker API call failed.')

                        except (ConnectionError, ValueError) as e:

                            # Failed to get price - SKIP current order
                            logger.debug('Manager: Failed to get price for {}: {}. Skipping buy order.'.format(
                                e, market))
                            skip_order(order, active_orders, completed_queue)
                            continue

                        bid_price = Decimal(str(ticker.get('Bid')))
                        ask_price = Decimal(str(ticker.get('Ask')))

                        # TODO: Change decimal value if buy orders are not getting filled
                        price_buffer = (((ask_price - bid_price) / bid_price) * Decimal('0.05')) + Decimal('1')

                        buy_price = (price_buffer * bid_price).quantize(BittrexConstants.DIGITS)

                        if buy_price > ask_price:
                            buy_price = bid_price

                        order.update_target_price(buy_price)

                        # No action if not enough to place buy order - SKIP current order
                        if wallet.get_quantity('BTC') < order.base_quantity:
                            logger.info('Manager: Not enough in wallet to place buy order. Skipping {}.'.format(market))
                            skip_order(order, active_orders, completed_queue)

                        try:
                            buy_response = bittrex.buy_limit(market, order.target_quantity, order.target_price)

                            if buy_response.get('success'):
                                order.update_uuid(buy_response.get('result').get('uuid'))
                                order.update_status(OrderStatus.EXECUTED.name)
                            else:
                                logger.info('Manager: Failed to buy {}.'.format(market))
                                raise ValueError('Manager: Execute buy order API call failed.')

                        except (ConnectionError, ValueError) as e:

                            # Failed to execute buy order - SKIP current order
                            logger.debug(
                                'Manager: Failed to execute buy order for {}: {}. Skipping buy order.'.format(
                                    market, e
                                ))

                            skip_order(order, active_orders, completed_queue)
                            continue

                # Actions for sell order
                else:
                    print(2)

                    sleep(1)

            stop = time()
            run_time = stop - start

            if run_time < sleep_time:
                sleep(sleep_time - run_time)

    except ConnectionError as e:
        logger.debug('ConnectionError: {}. Exiting ...'.format(e))
    finally:
        # TODO: check if any open orders
        # TODO: refactor based on wallet class
        if any(k for k in wallet if wallet.get(k) > 0 and k != 'BTC'):
            logger.info('Tradebot: Open positions remaining.')
            logger.info('Tradebot: Closing open positions.')
            close_positions(bittrex, wallet, logger)
        else:
            logger.info('FINAL WALLET AMOUNT: {}'.format(str(wallet.get('BTC'))))
            logger.info('Tradebot: No positions open. Safe to exit.')

        logger.info('Manager: Stopped manager.')


# TODO: implement telegram bot
def run_telegram(from_tradebot_queue, to_tradebot_queue):
    while True:

        order_data = from_tradebot_queue.get()

        if order_data.get('profit'):

            print('completed order')

        else:

            print('buy order')


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

    return jsonify("STOPPED SCRAPER"), 200


@app.route('/tradebot/set/wallet/<float:amount>', methods=['GET'])
def _tradebot_set_wallet(amount):
    main_logger.info('Detected TRADEBOT: SET WALLET endpoint.')

    global WALLET_TOTAL
    WALLET_TOTAL = Decimal(amount).quantize(BittrexConstants.DIGITS)

    message = 'Tradebot: Wallet total set successfully: {}'.format(str(WALLET_TOTAL))
    main_logger.info(message)

    return jsonify(message), 200


@app.route('/tradebot/set/call/<float:amount>', methods=['GET'])
def _tradebot_set_call(amount):
    main_logger.info('Detected TRADEBOT: SET CALL endpoint.')

    global AMOUNT_PER_CALL
    AMOUNT_PER_CALL = Decimal(amount).quantize(BittrexConstants.DIGITS)

    message = 'Tradebot: Amount per call set successfully: {}'.format(str(AMOUNT_PER_CALL))
    main_logger.info(message)

    return jsonify(message), 200


@app.route('/tradebot/start/<table_name>', methods=['GET'])
def _tradebot_start(table_name):
    main_logger.info('Crocket: Detected tradebot START endpoint.')

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
                       args=(TRADEBOT_QUEUE, SCRAPER_TRADEBOT_QUEUE, ORDER_QUEUE, COMPLETED_ORDER_QUEUE,
                             MARKETS, AMOUNT_PER_CALL, table_name, main_logger, SKIP_LIST))
    tradebot.start()

    global manager
    manager = Process(target=run_manager,
                      args=())  # TODO add arguments here
    manager.start()

    return jsonify("Tradebot: Started successfully. Wallet total: {}. Amount per call: {}".format(
        str(WALLET_TOTAL), str(AMOUNT_PER_CALL))), 200


@app.route('/tradebot/stop', methods=['GET'])
def _tradebot_stop():
    main_logger.info("Crocket: Detected tradebot STOP endpoint.")

    SCRAPER_QUEUE.put('STOP TRADEBOT')
    TRADEBOT_QUEUE.put('STOP')
    MANAGER_QUEUE.put('STOP')

    tradebot.join()
    manager.join()

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
    app.run(debug=False, port=9999)
finally:

    # ==========================================================================
    print('Server terminated.')
    # ==========================================================================
