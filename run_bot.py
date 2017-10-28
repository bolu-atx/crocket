from datetime import datetime
from json import load as json_load
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import logging

from bittrex.bittrex import Bittrex


def log_error_and_reply_text(message, update):

    logger.error(message)
    update.message.reply_text(message)


def start(bot, update):
    """Send a message when the command /start is issued."""
    update.message.reply_text('Hi!')


def check(bot, update, args):
    """Send a message when the command /help is issued."""

    if args:
        market = 'BTC-{}'.format(args[0]).upper()

        try:
            price = bittrex.get_ticker(market).get('result').get('Last')
            update.message.reply_text('{0}: {1:.8f}'.format(market, price))

        except (ConnectionError, AttributeError):
            error_message = 'Check failed.'
            log_error_and_reply_text(error_message, update)


def get_balances(bot, update):
    """
    Send a message of current balances
    :param bot:
    :param update:
    :return:
    """

    balances = {}
    balance_string = 'Balance:\n'
    available_string = 'Available:\n'

    try:
        response = bittrex.get_balances().get('result')
        for item in response:
            currency = item.get('Currency')
            balance = item.get('Balance')
            available = item.get('Available')

            if currency and balance > 0:
                balances[currency] = {'balance': balance,
                                      'available': available}

        for key, item in sorted(balances.items()):
            balance_string += '{}: {}\n'.format(key, item.get('balance'))
            available_string += '{}: {}\n'.format(key, item.get('available'))

        update.message.reply_text(balance_string)
        update.message.reply_text(available_string)

    except (ConnectionError, AttributeError):
        error_message = 'Failed to get balances.'
        log_error_and_reply_text(error_message, update)


def get_orders(bot, update, args):
    """
    Send a message of open orders
    :param bot:
    :param update:
    :param args:
    :return:
    """

    order_string = 'Open orders:\n'

    try:

        if args:
            market = args[0].upper()
        else:
            market = None

        response = bittrex.get_open_orders(market)

        if response.get('success'):

            for item in response.get('result'):
                currency = item.get('Exchange')
                rate = item.get('Limit')
                order_type = item.get('OrderType')
                quantity = item.get('Quantity')
                quantity_remaining = item.get('QuantityRemaining')

                order_string += '{0}: {1}\n' \
                                '{2}: {3:.8f}\n' \
                                '{4}: {5}\n' \
                                '{6}: {7}\n' \
                                '{8}: {9}\n\n'.format('Currency', currency,
                                                      'Rate', rate,
                                                      'Type', order_type,
                                                      'Quantity', quantity,
                                                      'Quantity Remaining', quantity_remaining)

            update.message.reply_text(order_string)

        else:
            update.message.reply_text('Failed to get open orders: {}.'.format(response.get('message')))

    except (ConnectionError, AttributeError):
        error_message = 'Failed to get orders.'
        log_error_and_reply_text(error_message, update)


def buy(bot, update, args):
    """
    Send a message on status of buy order
    :param bot:
    :param update:
    :param args:
    :return:
    """

    if args:

        # Validate input parameters
        if len(args) == 3 and args[0].startswith('BTC-') and args[1] > 0 and args[2] > 0:
            market = args[0].upper()
            quantity = args[1]
            rate = args[2]

            try:
                response = bittrex.buy_limit(market, quantity, rate)

                if response.get('success'):

                    buy_string = 'Buy successful.\n' \
                                 'UUID: {}'.format(response.get('result').get('uuid'))

                    update.message.reply_text(buy_string)
                else:
                    raise AttributeError

            except (ConnectionError, AttributeError):
                error_message = 'Buy failed.'
                log_error_and_reply_text(error_message, update)

        else:
            error_message = 'Buy failed: incorrect format.\n' \
                            'Ex: /buy BTC-LTC 100 0.000001'
            log_error_and_reply_text(error_message, update)

    else:
        error_message = 'Buy failed: no currency specified.'
        log_error_and_reply_text(error_message, update)


def sell(bot, update, args):
    """
    Send a message on status of sell order
    :param bot:
    :param update:
    :param args:
    :return:
    """

    if args:

        # Validate input parameters
        if len(args) == 3 and args[0].startswith('BTC-') and args[1] > 0 and args[2] > 0:
            market = args[0].upper()
            quantity = args[1]
            rate = args[2]

            try:
                response = bittrex.sell_limit(market, quantity, rate)

                if response.get('success'):

                    buy_string = 'Sell successful.\n' \
                                 'UUID: {}'.format(response.get('result').get('uuid'))

                    update.message.reply_text(buy_string)
                else:
                    raise AttributeError

            except (ConnectionError, AttributeError):
                error_message = 'Sell failed.'
                log_error_and_reply_text(error_message, update)

        else:
            error_message = 'Sell failed: incorrect format.\n' \
                            'Ex: /sell BTC-LTC 100 0.000001'
            log_error_and_reply_text(error_message, update)

    else:
        error_message = 'Sell failed: no currency specified.'
        log_error_and_reply_text(error_message, update)


def cancel(bot, update, args):
    """
    Send message on status of canceling order
    :param bot:
    :param update:
    :param args:
    :return:
    """

    if args:
        response = bittrex.cancel(args[0])

        if response.get('success'):
            update.message.reply_text('Cancel successful!')
        else:
            error_message = 'Cancel failed: UUID not recognized.'
            log_error_and_reply_text(error_message, update)
    else:
        error_message = 'Cancel failed: no UUID specified.'
        log_error_and_reply_text(error_message, update)


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)


def main():
    """Start the bot."""

    BITTREX_CREDENTIALS_PATH = '/Users/brian/crypto/crocket/bittrex_credentials.json'
    TELEGRAM_TOKEN_PATH = '/Users/brian/crypto/crocket/telegram_token.json'

    # Enable logging
    logging.basicConfig(filename='run_bot.{:%Y:%m:%d:%H:%M:%S}.log'.format(datetime.now()),
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    global logger
    logger = logging.getLogger(__name__)

    logger.info('Initialized logger.')

    # Load bittrex credentials
    try:
        with open(BITTREX_CREDENTIALS_PATH, 'r') as f:
            credentials = json_load(f)
    except FileNotFoundError:
        logger.error('Failed to load credentials from {}.'.format(BITTREX_CREDENTIALS_PATH))
        raise

    # Create Bittrex object
    global bittrex
    bittrex = Bittrex(api_key=credentials.get('key'), api_secret=credentials.get('secret'))

    # Telegram token
    try:
        with open(TELEGRAM_TOKEN_PATH, 'r') as f:
            telegram = json_load(f)
            token = telegram.get('token')
    except FileNotFoundError:
        logger.error('Failed to load token from {}.'.format(BITTREX_CREDENTIALS_PATH))
        raise

    # Create the EventHandler and pass it your bot's token.
    updater = Updater(token)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("check", check, pass_args=True))
    dp.add_handler(CommandHandler("getbalances", get_balances))
    dp.add_handler(CommandHandler("getorders", get_orders, pass_args=True))
    dp.add_handler(CommandHandler("buy", buy, pass_args=True))
    dp.add_handler(CommandHandler("sell", sell, pass_args=True))
    dp.add_handler(CommandHandler("cancel", cancel, pass_args=True))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling(clean=True)

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
