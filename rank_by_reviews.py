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

'''
Crawls shops reviews pages and collects reviews from each one.
After we collect from all review pages, we rank products based
on which has the most reviews
'''

## Set up session for proxycrawl req

session_counter = 0
next_session = random.randint(8, 20)
curent_session = False


def get_session():
    global curent_session
    if not curent_session or session_counter == next_session:
        print('getting new session...')
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
    token = 'YCi-hG8lIZzvSqMKxGK3gg'
    request_url = 'https://api.proxycrawl.com'
    url_param = parse.quote(url)
    max_tryouts = 10
    curent_try = 0
    while True:
        curent_try += 1
        response = session.get(f'{request_url}?token={token}&url={url_param}')
        print(f'got response code {response.status_code}..')
        if response.status_code not in [200]:
            print(f'### Received status {response.status_code} ###')
            print('Retrying...')
        else:
            break
        if curent_try == max_tryouts:
            raise MaxTryoutsExceeded('Too many responses with bad codes :(')
    return response


def make_normal_request(url):
    session = get_session()
    response = session.get(url)
    print(f'got response code {response.status_code}..')
    if response.status_code not in [200]:
        print(f'### Received status {response.status_code} ###')
        print('Retrying...')
    return response


## Utils funcs for getting/setting the data


## Make the crawling

names = ['PrevntProducts']
links_struct = 'https://www.etsy.com/shop/{}/reviews'

scanned_pages = defaultdict(dict)
scanned_pages_responses = defaultdict(dict)


def start_crawling():
    try:
        for shop_name in names:
            print(f'Processing {shop_name}')
            url = links_struct.format(shop_name)
            while True:
                print(f'scanning url {url}')
                scanned_pages[shop_name][url] = []

                ## when running change to proxy request
                response = make_proxycrawl_request(url)
                scanned_pages_responses[shop_name][url] = response if response.status_code == 200 else False
                bs = BeautifulSoup(response.text)
                try:
                    url = bs.find('span', string=re.compile('Next page')).parent['href']
                except KeyError:
                    print('-' * 10)
                    print('No search button found! End crawling for this shop')
                    break
        with open('reviews_scanned_pages_done.pkl', 'wb') as pickle_file:
            pickle.dump(scanned_pages, pickle_file)
        with open('reviews_scanned_pages_responses.pkl', 'wb') as pickle_file:
            pickle.dump(scanned_pages_responses, pickle_file)
    except MaxTryoutsExceeded:
        with open('reviews_scanned_pages_blocked.pkl', 'wb') as pickle_file:
            pickle.dump(scanned_pages, pickle_file)
    except Exception as e:
        print(f'exception {str(e)}')
        with open('reviews_scanned_pages_exception.pkl', 'wb') as pickle_file:
            pickle.dump(scanned_pages, pickle_file)


def get_date(date_str):
    formats = ['%b %d, %Y', '%d %b, %Y']
    for format in formats:
        try:
            return datetime.strptime(date_str, format)
        except:
            pass
    raise ValueError(f'Encountered unknown date format: {date_str}')



def extract_review_data(review_bs):
    try:
        href = review_bs.find('a', href=re.compile(r'/listing/'))
        if href is None:
            return False
        puid = re.search('\/listing\/([\S]*)\/', href['href']).groups()[0]
        date_str = \
        re.search(' on ([\s\S]*)\\n', review_bs.find('p', {'class': 'shop2-review-attribution'}).text).groups()[0]
        date_obj = get_date(date_str)
        return {
            'puid': puid,
            'date': date_obj
        }
    except Exception as e:
        print(e)
        return False


def process_responses():
    # open scanned pages file
    with open('reviews_scanned_pages_responses.pkl', 'rb') as file:
        all_responses = pickle.load(file)
    # process
    processed_reviews = defaultdict(dict)
    for shop_name, responses in all_responses.items():
        for url, response in responses.items():
            if response:
                processed_reviews[shop_name][url] = []
                bs = BeautifulSoup(response.content)
                reviews = bs.find('ul', {'class': 'reviews-list'}).find_all('li')
                for review in reviews:
                    review_data = extract_review_data(review)
                    if review_data:
                        processed_reviews[shop_name][url].append(
                            review_data
                        )
    return processed_reviews


## do the ranking

def get_sorted_list(products_list) -> list:
    return sorted(products_list.items(), key=lambda x: x[1]['count'], reverse=True)


def product_url(puid):
    return f'https://www.etsy.com/listing/{puid}'


# start_crawling()
processed_reviews = process_responses()

with open('ranking_by_reviews_results.txt', 'w') as results_file:
    now_datetime = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    results_file.write(f'Results of crawling on date {now_datetime}:\n')
    results_file.write(f'\n')
    for shop_name, pages in processed_reviews.items():
        ranked_products = defaultdict(dict)
        results_file.write(f'Shop {shop_name}\n')
        results_file.write(f'================================================\n')

        ## aggregate the reviews data
        for review_data in reduce(lambda prods, page: prods + page, pages.values(), []):
            puid = review_data['puid']
            if puid in ranked_products:
                ranked_products[puid]['count'] += 1
                ranked_products[puid]['dates'].append(review_data['date'])
            else:
                ranked_products[puid] = {
                    'count': 1,
                    'dates': [review_data['date'], ]
                }

        ## output report
        # prepared_reviews = get_sorted_list(ranked_products, 20);
        prepared_reviews = filter(lambda rev: rev[1]['count']>1,get_sorted_list(ranked_products))

        for puid, data in prepared_reviews:
            count = data['count']
            review_dates = reduce(lambda dates_str, date: dates_str + ' -- ' + date.strftime('%Y-%b-%d'), data['dates'], '')
            results_file.write(f' --- {count} reviews for product {product_url(puid)} on dates ::: {review_dates}\n')
