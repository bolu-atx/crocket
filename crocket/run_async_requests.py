import sys
sys.path.insert(1, '/Users/brian/crypto/crocket')

from json import load as json_load
from pprint import pprint
from random import shuffle
from time import sleep, time
from requests_futures.sessions import FuturesSession
from requests.exceptions import ConnectTimeout, ProxyError

from crocket.bittrex.bittrex2 import Bittrex, return_request_input

PROXY_LIST_PATH = '/Users/brian/crypto/crocket/proxy_list.txt'
BITTREX_CREDENTIALS_PATH = '/Users/brian/bittrex_credentials.json'


def configure_ip(ip):
    return {
        'http': ip,
        'https': ip
    }


def process_response(session, response):
    try:
        response.data = response.json()
    except Exception as e:
        print(e)
        response.data = {
            'success': False,
            'message': 'NO_API_RESPONSE',
            'result': None
        }


# Read files

with open(PROXY_LIST_PATH, 'r') as f:
    proxies = f.read().splitlines()

with open(BITTREX_CREDENTIALS_PATH, 'r') as f:
    credentials = json_load(f)

# Create bittrex objects

b1 = Bittrex(api_key=credentials.get('key'),
             api_secret=credentials.get('secret'),
             api_version='v2.0')

b2 = Bittrex(api_key=credentials.get('key'),
             api_secret=credentials.get('secret'),
             dispatch=return_request_input,
             api_version='v2.0')

# Get list of currencies

response = b1.get_markets()

currencies = [x.get('MarketName') for x in response.get('result')
              if x.get('BaseCurrency') == 'BTC' and x.get('IsActive')]

markets = currencies

rind = list(range(200))

# Test asynchronous requests

with FuturesSession(max_workers=200) as session:

    for ii in range(0, 1):  # TODO: replace with while loop

        shuffle(rind)

        inputs = []
        responses1 = []

        start = time()

        for index in range(len(markets)):
            request_input = b2.get_market_history(markets[index])
            inputs.append(request_input)

            proxy = configure_ip(proxies[rind[index]])

            future = session.get(request_input.get('url'),
                                 background_callback=process_response,
                                 headers={"apisign": request_input.get('apisign')},
                                 timeout=5,
                                 proxies=proxy)

            responses1.append([markets[index], future])

        stop = time()
        print('Total elapsed time: {}s'.format(str(stop - start)))

        pprint(responses1)

        for index in range(len(markets)):
            try:
                responses1[index][1] = responses1[index][1].result()
            except (ProxyError, ConnectTimeout):
                print('Failed API call for {}, index: {}.'.format(markets[index], str(index)))

                proxy = configure_ip(proxies[rind[index]])
                future = session.get(inputs[index].get('url'),
                                     background_callback=process_response,
                                     headers={"apisign": inputs[index].get('apisign')},
                                     timeout=5,
                                     proxies=proxy)

                responses1[index][1] = responses1[index][1].result()

        sleep(30)