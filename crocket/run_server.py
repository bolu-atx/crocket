from datetime import datetime
from flask import Flask, jsonify
from logging import FileHandler, Formatter, StreamHandler, getLogger
from multiprocessing import Process, Queue
from os import environ
from os.path import dirname, join, realpath
from time import sleep

from sql.sql import Database
from utilities.credentials import get_credentials


# # ==============================================================================
# # Initialize logger
# # ==============================================================================
# logger = getLogger('scraper')
#
# logger.setLevel(10)
#
# fh = FileHandler(
#     '/var/tmp/scraper.{:%Y:%m:%d:%H:%M:%S}.log'.format(datetime.now()))
# fh.setFormatter(Formatter('%(asctime)s:%(name)s:%(levelname)s: %(message)s'))
# logger.addHandler(fh)
#
# sh = StreamHandler()
# sh.setFormatter(Formatter('%(levelname)s: %(message)s'))
# logger.addHandler(sh)
#
# logger.info('Initialized logger.')
#
# ==============================================================================
# Set up environment variables
# ==============================================================================

HOME_DIRECTORY_PATH = environ['HOME']

CREDENTIALS_FILE_PATH = join(HOME_DIRECTORY_PATH, '.credentials_unlocked.json')

BITTREX_CREDENTIALS_PATH = join(HOME_DIRECTORY_PATH, 'bittrex_credentials.json')

CROCKET_DIRECTORY = dirname(realpath(__file__))

PROXY_LIST_PATH = join(CROCKET_DIRECTORY, 'proxy_list.txt')

MARKETS_LIST_PATH = join(CROCKET_DIRECTORY, 'markets.txt')

HOSTNAME = 'localhost'

USERNAME, PASSCODE = get_credentials(CREDENTIALS_FILE_PATH)


app = Flask(__name__)

# ==============================================================================
# Set up queues
# ==============================================================================

SCRAPER_QUEUE = Queue()
TRADEBOT_QUEUE = Queue()
SCRAPER_TRADEBOT_QUEUE = Queue()

# ==============================================================================
# Helper functions
# ==============================================================================


def initialize_database(connection, database_name):

    # TODO: Add create database if does not exist

    # Create tables if does not exist
    for market in MARKETS:
        connection.create_price_table(market)


# ==============================================================================
# Run functions
# ==============================================================================


def run_scraper(control_queue, database_name):

    # Initialize database connection
    db = Database(hostname=HOSTNAME,
                  username=USERNAME,
                  password=PASSCODE,
                  database_name=database_name,
                  logger=logger)

    initialize_database(db, database_name)

    run_tradebot = False



    # Initialize variables
    num_proxies = len(PROXIES)
    randoms = list(range(num_proxies))

    MAX_API_RETRY = 3

    response_dict, working_data = {}, {}

    futures = []

    current_datetime = datetime.now().astimezone(tz=None)
    current_datetime = {k: current_datetime for k in MARKETS}
    last_price = {k: Decimal(0) for k in MARKETS}
    weighted_price = {k: Decimal(0) for k in MARKETS}

    try:

        with FuturesSession(max_workers=10) as session:

            while True:
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
                                           timeout=3,
                                           proxies=proxy)

                    # Add attributes to response
                    response.market = market
                    response.url = request_input.get('url')
                    response.headers = headers

                    futures.append(response)

                for future in as_completed(futures):

                    try:
                        response_data = future.result().data

                        if not response_data.get('success'):
                            if response_data.get('message') == "INVALID_MARKET":
                                MARKETS.remove(future.market)
                                logger.debug('Removed {}: invalid market ...'.format(future.market))
                            continue

                        response_dict[future.market] = response_data.get('result')
                        if not response_dict[future.market]:
                            if response_data.get('message') == "NO_API_RESPONSE":
                                raise ProxyError('NO API RESPONSE')

                    except (ProxyError, ConnectTimeout, ConnectionError, ReadTimeout):

                        api_retry = 0

                        while True:

                            if api_retry >= MAX_API_RETRY:
                                logger.debug('MAX API RETRY LIMIT ({}) REACHED. SKIPPING {}.'.format(str(MAX_API_RETRY),
                                                                                                     future.market))
                                break

                            r = randint(0, num_proxies - 1)
                            proxy = configure_ip(PROXIES[r])

                            try:
                                response = session.get(future.url,
                                                       background_callback=process_response,
                                                       headers=future.headers,
                                                       timeout=2,
                                                       proxies=proxy)
                                response_dict[future.market] = response.result().data.get('result')
                                if not response_dict[future.market]:
                                    logger.debug('NO API RESPONSE, RETRYING: {} ...'.format(future.market))
                                    api_retry += 1
                                    continue

                                break

                            except (ProxyError, ConnectTimeout, ConnectionError, ReadTimeout):
                                api_retry += 1
                                logger.debug('Retried API call failed for {}.'.format(future.market))

                working_data, current_datetime, last_price, weighted_price, entries = \
                    process_data(response_dict, working_data, current_datetime, last_price, weighted_price, logger,
                                 interval)

                if entries:
                    db.insert_transaction_query(entries)

                stop = time()
                run_time = stop - start

                del futures[:]

                if run_time < sleep_time:
                    sleep(sleep_time - run_time)

                    # TODO: At midnight of every day - check and delete if any data past 30 days

    except (KeyboardInterrupt, ConnectionError) as e:
        logger.debug('Error: {}. Exiting ...'.format(e))

    db.close()

    while True:
        sleep(2)
        print('INSIDE LOOP')

        if run_tradebot:
            print("Scraper: Passing {} to tradebot.".format(str(num)))
            SCRAPER_TRADEBOT_QUEUE.put(num)

        if not control_queue.empty():

            signal = control_queue.get()

            if signal == "START TRADEBOT":
                run_tradebot = True
                print("Scraper: Starting tradebot.")
            elif signal == "STOP TRADEBOT":
                run_tradebot = False
            elif signal == "STOP":
                print("Scraper: Stopping scraper.")
                break

        num += 1

    print("Scraper: EXITED LOOP.")


def run_tradebot(control_queue, data_queue):

    status_dict = {}

    while True:

        scraper_data = data_queue.get()

        print("TRADEBOT: Received {} from scraper.".format(str(scraper_data)))

        if not control_queue.empty():

            signal = control_queue.get()

            if signal == "STOP":
                print("Tradebot: Stopping tradebot.")
                break

    print("Tradebot: EXITED LOOP.")


# ==============================================================================
# Endpoints
# ==============================================================================


@app.route('/scraper/start/<database_name>', methods=['GET'])
def _scraper_start(database_name):

    print('Reached START SCRAPER endpoint.')
    print('Starting scraper using database: {}.'.format(database_name))

    scraper = Process(target=run_scraper, args=(SCRAPER_QUEUE,database_name))
    scraper.start()

    return jsonify("STARTED SCRAPER"), 200


@app.route('/scraper/stop', methods=['GET'])
def _scraper_stop():

    print('Reached STOP SCRAPER endpoint.')

    SCRAPER_QUEUE.put("STOP")

    return jsonify("STOPPED SCRAPER"), 200


@app.route('/tradebot/start', methods=['GET'])
def _tradebot_start():

    print('Reached START TRADEBOT endpoint.')

    SCRAPER_QUEUE.put("START TRADEBOT")

    tradebot = Process(target=run_tradebot, args=(TRADEBOT_QUEUE, SCRAPER_TRADEBOT_QUEUE))
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
