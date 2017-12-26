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
from manager_helper import buy_above_bid, get_order_and_update_wallet, sell_below_ask, skip_order
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


def initialize_databases(database_name, markets,
                         logger=None):
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


def format_tradebot_entry(market, buy_time, buy_signal, buy_price, buy_total, sell_time, sell_signal, sell_price,
                          sell_total, profit, percent):
    """
    Format trade entry for insertion into trade table
    :param market: Name of market
    :param buy_time: Close time of buy order
    :param buy_signal: Price when buy signal is triggered
    :param buy_price: Actual price of executed buy order
    :param buy_total: Net total of buy order
    :param sell_time: Close time of sell order
    :param sell_signal: Price when sell signal is triggered
    :param sell_price: Actual price of executed sell order
    :param sell_total: Net total of sell order
    :param profit: Net profit (sell total - buy total)
    :param percent: Net percent ((sell total - buy total) / buy total)
    :return:
    """

    return [('market', market),
            ('buy_time', buy_time),
            ('buy_signal', buy_signal),
            ('buy_price', buy_price),
            ('buy_total', buy_total),
            ('sell_time', sell_time),
            ('sell_signal', sell_signal),
            ('sell_price', sell_price),
            ('sell_total', sell_total),
            ('profit', profit),
            ('percent', percent)]


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
                interval=60,
                sleep_time=5):
    """
    Run scraper to pull data from Bittrex
    :param control_queue: Queue to control scraper
    :param database_name: Name of database
    :param logger: Main logger
    :param markets: List of active markets
    :param interval: Duration between entries into database
    :param sleep_time: Duration between API calls
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

                stop = time()
                run_time = stop - start
                logger.info('Tradebot: Total time: {0:.2f}s'.format(run_time))

                if run_time < sleep_time:
                    sleep(sleep_time - run_time)

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

                        # Check if buy order skipped
                        if order.status == OrderStatus.SKIPPED.name:
                            market_status[order_market].bought = False
                            market_status[order_market].buy_signal = None
                        else:
                            market_status[order_market].buy_order = order
                    else:
                        market_status[order_market].sell_order = order

                completed_orders.clear()

            for market in scraper_data.keys():

                data = market_data.get(market)

                if market not in skip_list:
                    if len(data.datetime) > 65:

                        # Clear the first entries
                        data.clear_first()

                        status = market_status.get(market)

                        run_algorithm(data,
                                      status,
                                      amount_per_call,
                                      pending_order_queue,
                                      logger)

                        # Completed buy and sell order for single market
                        if status.buy_order.status == OrderStatus.COMPLETED.name and \
                                status.sell_order.status == OrderStatus.COMPLETED.name:
                            profit = (status.sell_order.total - status.buy_order.total).quantize(
                                BittrexConstants.DIGITS)
                            percent = (profit * Decimal(100) / status.buy_order.total).quantize(Decimal(10) ** -4)

                            formatted_buy_time = format_time(status.buy_order.closed_time, "%Y-%m-%d %H:%M:%S")
                            formatted_sell_time = format_time(status.sell_order.closed_time, "%Y-%m-%d %H:%M:%S")

                            logger.info('Tradebot: completed buy/sell order for {}.'.format(market))

                            db.insert_query(table_name, format_tradebot_entry(market,
                                                                              formatted_buy_time,
                                                                              status.buy_signal,
                                                                              status.buy_order.actual_price,
                                                                              status.buy_order.total,
                                                                              formatted_sell_time,
                                                                              status.sell_signal,
                                                                              status.sell_order.acutal_price,
                                                                              status.sell_order.total,
                                                                              profit,
                                                                              percent))

                            # Reset buy/sell orders and buy/sell signals
                            status.clear_orders()
                            status.buy_signal = None
                            status.sell_signal = None

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
        logger.info('Tradebot: Database connection closed.')


def run_manager(control_queue, order_queue, completed_queue, wallet_total, logger,
                sleep_time=5):
    """
    Handles execution of all orders
    :param control_queue: Queue to control manager
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
    wallet = Wallet(amount=wallet_total,
                    markets=MARKETS)

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

                active_orders += new_orders
                new_orders.clear()

            start = time()

            for order in active_orders:

                market = order.market

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
                                    if order_data.get('QuantityRemaining') > 0:
                                        cancel_response = bittrex.cancel(order.uuid)

                                        logger.info('Manager: {} buy order incomplete - canceling.'.format(market))

                                        if not cancel_response.get('success'):
                                            # TODO: add telegram message here
                                            logger.error('Manager: Failed to cancel buy order for {}.'.format(market))
                                            logger.error('Manager: May need to manually cancel order.')

                                        # Check if none of buy order filled
                                        if order_data.get('QuantityRemaining') == order_data.get('Quantity'):
                                            logger.info(
                                                'Manager: {} buy order not filled - canceled and skipped.'.format(
                                                    market))
                                            skip_order(order, active_orders, completed_queue)
                                            continue

                                        logger.info('Manager: {} buy order partially filled.'.format(market))

                                    order.add_completed_order(order_data)

                                    wallet.update_wallet(market, order.current_quantity, order.total)

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

                        buy_status = buy_above_bid(market, order, wallet, bittrex, logger, percent=5)

                        if not buy_status:
                            skip_order(order, active_orders, completed_queue)

                # Actions for sell order
                else:
                    if order.status == OrderStatus.EXECUTED.name:
                        try:
                            order_response = bittrex.get_order(order.uuid)

                            if order_response.get('success'):
                                order_data = order_response.get('result')

                                # First time getting order data
                                if order.open_time is not None:
                                    order.open_time = utc_to_local(convert_bittrex_timestamp_to_datetime(
                                        order_data.get('Opened')))

                                # Check if sell order has executed or passed open duration
                                if not order_data.get('IsOpen') or \
                                        (datetime.now().astimezone(tz=None) - order.open_time).total_seconds() > 60:

                                    # Sell order is not complete - cancel order
                                    if order_data.get('QuantityRemaining') > 0:
                                        cancel_response = bittrex.cancel(order.uuid)

                                        logger.info('Manager: {} sell order incomplete - canceling.'.format(market))

                                        if not cancel_response.get('success'):
                                            # TODO: add telegram message here
                                            logger.error('Manager: Failed to cancel buy order for {}.'.format(market))
                                            logger.error('Manager: May need to manually cancel order.')

                                        # Check if any of sell order completed
                                        if order_data.get('QuantityRemaining') != order_data.get('Quantity'):

                                            order.add_completed_order(order_data)
                                            wallet.update_wallet(market, -1 * order.current_quantity, -1 * order.total)
                                            logger.info('Manager: {} sell order partially filled.'.format(market))

                                        # Market sell
                                        sell_status = sell_below_ask(market, order, wallet, bittrex, logger,
                                                                     percent=110)

                                        logger.info('Manager: Executing market sell order for {}.'.format(market))

                                        if not sell_status:
                                            skip_order(order, active_orders, completed_queue)

                                        continue

                                    order.add_completed_order(order_data)
                                    wallet.update_wallet(market, -1 * order.current_quantity, -1 * order.total)

                                    logger.info('WALLET AMOUNT: {} BTC'.format(str(wallet.get_quantity('BTC'))))
                                    logger.info('WALLET AMOUNT: {} {}'.format(str(wallet.get_base_quantity(market)),
                                                                              market.split('-')[-1]))

                                    active_orders.remove(order)
                                    completed_queue.put(order)

                            else:
                                raise ValueError('Manager: Get sell order data API call failed.')

                        except (ConnectionError, ValueError) as e:

                            # Failed to get sell order data - SKIP current order
                            logger.debug('Manager: Failed to get sell order data for {}: {}.'.format(market, e))

                            cancel_response = bittrex.cancel(order.uuid)

                            if not cancel_response.get('success'):
                                # TODO: send telegram message
                                logger.error('Manager: Failed to cancel buy order for {}.'.format(market))
                                logger.error('Manager: May need to manually cancel order.')

                            skip_order(order, active_orders, completed_queue)

                    # Sell order has not been executed
                    else:

                        sell_status = sell_below_ask(market, order, wallet, bittrex, logger, percent=5)

                        if not sell_status:
                            skip_order(order, active_orders, completed_queue)

            stop = time()
            run_time = stop - start
            logger.info('Manager: Run time: {0:.2f}s'.format(run_time))

            if run_time < sleep_time:
                sleep(sleep_time - run_time)

            if not control_queue.empty():

                signal = control_queue.get()

                if signal == 'STOP':
                    logger.info('Manager: Stopping manager.')
                    break

    except ConnectionError as e:
        logger.debug('ConnectionError: {}. Exiting ...'.format(e))
    finally:

        if active_orders:

            logger.info('Manager: {} active orders.'.format(str(len(active_orders))))
            logger.info('Manager: Closing open positions.')

            # Cancel orders and execute market sells
            for order in active_orders:

                if order.status == OrderStatus.EXECUTED.name:

                    cancel_response = bittrex.cancel(order.uuid)

                    if not cancel_response.get('success'):
                        logger.info('Manager: Cancel order failed for {}'.format(order.market))

                    get_order_and_update_wallet(order, wallet, bittrex)

                sell_status = sell_below_ask(order.market, order, wallet, bittrex, logger, percent=110)

                if not sell_status.get('success'):
                    logger.info('Manager: Market sell order failed for {}.'.format(order.market))

                sleep(3)  # Wait for market sell order to complete

                # Get status of all market sell orders
                get_order_and_update_wallet(order, wallet, bittrex)

            open_markets = wallet.get_open_markets()

            if open_markets:
                logger.info('Manager: Remaining open markets:\n', open_markets)

            logger.info('FINAL WALLET AMOUNT: {}'.format(str(wallet.get_quantity('BTC'))))
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
                      args=(MANAGER_QUEUE, ORDER_QUEUE, COMPLETED_ORDER_QUEUE,
                            WALLET_TOTAL, main_logger))
    manager.start()

    return jsonify("Tradebot/Manager: Started successfully. Wallet total: {}. Amount per call: {}".format(
        str(WALLET_TOTAL), str(AMOUNT_PER_CALL))), 200


@app.route('/tradebot/stop', methods=['GET'])
def _tradebot_stop():
    main_logger.info("Crocket: Detected tradebot STOP endpoint.")

    SCRAPER_QUEUE.put('STOP TRADEBOT')
    TRADEBOT_QUEUE.put('STOP')
    MANAGER_QUEUE.put('STOP')

    tradebot.join()
    manager.join()

    return jsonify("STOPPED TRADEBOT AND MANAGER"), 200


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
