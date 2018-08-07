import requests

try:
    import simplejson as json
except ImportError:
    import json

import logging
logger = logging.getLogger(__name__)


def list_accounts(endpoint, user, key):
    headers = {'X-Auth-Admin-User': user,
               'X-Auth-Admin-Key': key}
    uri = '%s/v2' % endpoint
    r = requests.get(uri, headers=headers, verify=False)
    logger.info(uri)
    logger.info(headers)
    logger.info("RESPONSE")
    logger.info(r)
    accounts = json.loads(r.content)['accounts']

    return [get_account_id(endpoint, user, key, account['name'])
            for account in accounts]


def get_account_id(endpoint, user, key, account_name):
    headers = {'X-Auth-Admin-User': user,
               'X-Auth-Admin-Key': key}
    uri = '%s/v2/%s' % (endpoint, account_name)
    r = requests.get(uri, headers=headers, verify=False)
    try:
        return json.loads(r.content)['account_id']
    except Exception:
        pass
