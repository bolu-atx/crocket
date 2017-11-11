
def configure_ip(ip):
    return {
        'http': ip,
        'https': ip
    }


def process_response(session, response):

    try:
        response.data = response.json()

    except:
        response.data = {
            'success': False,
            'message': 'NO_API_RESPONSE',
            'result': None
        }