import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import re
import time
import random
from urllib import parse
import pickle
from functools import reduce
from datetime import datetime
import logging

## Set up session for proxycrawl req

session_counter = 0
next_session = random.randint(8, 20)
curent_session = False


def get_session():
    global curent_session
    if not curent_session or session_counter == next_session:
        logging.debug('getting new session...')
        session = requests.session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'
        })
        curent_session = session
    return curent_session


## ProxyCrawl request

class MaxTryoutsExceeded(Exception):
    pass


def make_proxycrawl_request(url):
    session = get_session()
    # token = 'YCi-hG8lIZzvSqMKxGK3gg'
    # request_url = 'https://api.proxycrawl.com?token={token}&url={url_param}'
    token = 'df39af56fa69499ff908fb67b2c11119'
    request_url = f'http://api.scraperapi.com?api_key={token}&url='
    url_param = parse.quote(url)
    max_tryouts = 20
    curent_try = 0
    while True:
        curent_try += 1
        response = session.get(request_url+url_param)
        logging.debug(f'got response code {response.status_code}..')
        if response.status_code not in [200]:
            logging.debug(f'### Received status {response.status_code} ###')
            logging.debug('Retrying...')
        else:
            break
        if curent_try == max_tryouts:
            raise MaxTryoutsExceeded('Too many responses with bad codes :(')
    return response


def make_normal_request(url):
    session = get_session()
    response = session.get(url)
    logging.debug(f'got response code {response.status_code}..')
    if response.status_code not in [200]:
        logging.debug(f'### Received status {response.status_code} ###')
        logging.debug('Retrying...')
    return response

def get_date(date_str):
    formats = ['%b %d, %Y', '%d %b, %Y']
    for format in formats:
        try:
            return datetime.strptime(date_str, format)
        except:
            pass
    raise ValueError(f'Encountered unknown date format: {date_str}')