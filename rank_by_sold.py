from bs4 import BeautifulSoup
from collections import defaultdict
import re
import pickle
from datetime import datetime
import logging

from utils import MaxTryoutsExceeded, make_proxycrawl_request
'''
Crawls shops reviews pages and collects reviews from each one.
After we collect from all review pages, we rank products based
on which has the most reviews
'''

logging.basicConfig(filename="rank_by_sold.log", level=logging.DEBUG)


## Make the crawling
STATUS_FINISHED = 'FINISHED'
STATUS_ERROR = 'ERROR'

def pickle_pages(scanned_pages):
    with open('sold_scanned_pages.pkl', 'wb') as pickle_file:
        logging.debug('...saving to disk...')
        pickle.dump(scanned_pages, pickle_file)

def start_crawling(url, scanned_pages):
    error=False
    backup_counter = 0
    try:
        while True:
            logging.debug(f'scanning url {url}')

            ## when running change to proxy request
            response = make_proxycrawl_request(url)
            scanned_pages.append({
                'url': url,
                'response': response if response.status_code == 200 else False
            })
            ## search for the next url
            bs = BeautifulSoup(response.text, features="html.parser")
            backup_counter += 1
            if backup_counter == 5:
                pickle_pages(scanned_pages)
                backup_counter = 0
            try:
                url = bs.find('span', string=re.compile('Next page')).parent['href']
            except KeyError:
                logging.debug('-' * 10)
                logging.debug('No search button found! End crawling for this shop')
                scanned_pages.append({
                    'response': STATUS_FINISHED,
                    'url': url
                })
                pickle_pages(scanned_pages)
                break
    except MaxTryoutsExceeded as e:
        logging.error('=== CRITICAL ERROR ===')
        logging.error(e)
        error = True
    except Exception as e:
        logging.error('=== CRITICAL ERROR ===')
        logging.error(e)
        error = True

    if error:
        raise Exception('Error in crawling!')
    return scanned_pages






def get_products(url):
    # open scanned pages file and pick up from the last successfully crawled page
    crawled_pages = []
    products = defaultdict(int)
    try:
        with open('sold_scanned_pages.pkl', 'rb') as file:
            responses = pickle.load(file)
            if responses[-1]['response'] != STATUS_FINISHED:
                url = responses[-1]['url']
                holder_array = responses[:-1]
            else:
                crawled_pages = responses
    except FileNotFoundError:
        holder_array = []
        crawled_pages = start_crawling(url, holder_array)
    if crawled_pages[-1]['response'] == STATUS_FINISHED:
        for page in crawled_pages[:-1]:
            # extract products
            bs = BeautifulSoup(page['response'].text, features="html.parser")
            for product_card in bs.findAll('a', {'class': 'listing-link'}):
                pid = product_card['data-listing-id']
                products[pid] += 1
        return products


## do the ranking

def get_filtered_list(products_list) -> list:
    return sorted(products_list.items(), key=lambda x: x[1], reverse=True)[:40]


def product_url(puid):
    return f'https://www.etsy.com/listing/{puid}'



shop_name='BluebirdApparel'
links_struct = 'https://www.etsy.com/shop/{}/sold'
logging.debug(f'Processing {shop_name}')
url = links_struct.format(shop_name)

processed_products = get_products(url)

with open('ranking_by_sold_results.txt', 'w') as results_file:
    now_datetime = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    results_file.write(f'Results of crawling on date {now_datetime}:\n')
    results_file.write(f'\n')
    results_file.write(f'Shop {shop_name}\n')
    results_file.write(f'================================================\n')
    for product in get_filtered_list(processed_products):
        results_file.write(f"{product[1]} sold of {product_url(product[0])}\n")