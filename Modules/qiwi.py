import localapi
import configparser
import simplejson

config = configparser.ConfigParser()
config.read('config.ini')
api_access_token = config['qiwi']['token']
number = config['qiwi']['phone_number']


async def check(data: int):
    session = localapi.Session()
    session.headers['authorization'] = 'Bearer ' + api_access_token
    parameters = {'rows': 50, 'type': 'IN'}
    payments = session.get('https://edge.qiwi.com/payment-history/v2/persons/' + number + '/payments', params=parameters)
    try:
        result = payments.json()

        for i in result['data']:
            if i['comment'] == str(data) and i['status'] == 'SUCCESS':
                return [True, i['total']['amount'], i['txnId']]

            if i['comment'] == str(data):
                return i['status']

            else:
                continue
        return None
    except simplejson.errors.JSONDecodeError:
        return None
