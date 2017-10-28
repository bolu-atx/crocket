from datetime import datetime
from json import load as json_load
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import logging

from bittrex.bittrex import Bittrex

# Define a few command handlers. These usually take the two arguments bot and
# update. Error handlers also receive the raised TelegramError object in error.

def start(bot, update):
    """Send a message when the command /start is issued."""
    update.message.reply_text('Hi!')


def check(bot, update, args):
    """Send a message when the command /help is issued."""

    if args:
        market = 'BTC-{}'.format(args[0]).upper()

        try:
            price = bittrex.get_ticker(market).get('result').get('Last')
            update.message.reply_text(market + ': {0:.8f}'.format(price))

        except (ConnectionError, AttributeError):
            logger.error('Check {} failed.'.format(market))
            update.message.reply_text('Check failed.')


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
        result = bittrex.get_balances().get('result')
        for item in result:
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
        logger.error('Failed to get balances.')
        update.message.reply_text('Get balances failed.')


def buy(bot, update, args):
    """Send a message when the command /help is issued."""

    if args:
        market = 'BTC-{}'.format(args[0]).upper()

        try:
            price = bittrex.get_ticker(market).get('result').get('Last')
            update.message.reply_text(market + ': {0:.8f}'.format(price))

        except (ConnectionError, AttributeError):
            logger.error('Buy {} failed.'.format(market))
            update.message.reply_text('Buy failed.')


def buynow(bot, update, args):
    """Send a message when the command /help is issued."""

    if args:
        market = 'BTC-{}'.format(args[0]).upper()

        try:
            price = bittrex.get_ticker(market).get('result').get('Last')
            update.message.reply_text(market + ': {0:.8f}'.format(price))

        except (ConnectionError, AttributeError):
            logger.error('Check {} failed.'.format(market))
            update.message.reply_text('Check failed.')


def echo(bot, update):
    """Echo the user message."""
    update.message.reply_text(update.message.text)


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
                        level=logging.DEBUG)

    global logger
    logger = logging.getLogger(__name__)

    logger.debug('Initialized logger.')

    # Load bittrex credentials
    try:
        with open(BITTREX_CREDENTIALS_PATH, 'r') as f:
            credentials = json_load(f)
    except FileNotFoundError:
        logger.debug('Failed to load credentials from {}.'.format(BITTREX_CREDENTIALS_PATH))
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
        logger.debug('Failed to load token from {}.'.format(BITTREX_CREDENTIALS_PATH))
        raise

    # Create the EventHandler and pass it your bot's token.
    updater = Updater(token)

    # Get the dispatcher to register handlers
    dp = updater.dispatcher

    # on different commands - answer in Telegram
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("check", check, pass_args=True))
    dp.add_handler(CommandHandler("getbalances", get_balances))

    # on noncommand i.e message - echo the message on Telegram
    dp.add_handler(MessageHandler(Filters.text, echo))

    # log all errors
    dp.add_error_handler(error)

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()