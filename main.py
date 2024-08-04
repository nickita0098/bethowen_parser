import csv
import configparser
from urllib.parse import quote
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

config = configparser.ConfigParser()
config.read('config.ini')

TOWN = config.get('Region', 'town')
REGION = config.get('Region', 'region')
CATEGORIES_URL = config.get('Urls', 'categories_url')
ITEMS_URL = config.get('Urls', 'items_url')
HEADERS = {
    'User-Agent': config.get('Headers', 'User-Agent'),
    'BETHOWEN_GEO_TOWN': quote(TOWN),
    'BETHOWEN_GEO_TOWN_ID': REGION
}
PROXIES = {
    'https': config.get('Proxies', 'https')
}
CATEGORY_ID = config.get('Category', 'category_id')

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_data(url, params=None):
    response = requests.get(url=url, headers=HEADERS, proxies=PROXIES, params=params)
    response.raise_for_status()
    return response.json()

def get_category_list(url):
    def extract_ids(subcategories):
        ids = []
        for category in subcategories:
            if 'subcategories' in category and category['subcategories']:
                ids.extend(extract_ids(category['subcategories']))
            else:
                ids.append(category['id'])
        return ids

    data = fetch_data(url)
    main_categories = data.get('categories', [])
    return extract_ids(main_categories)  # -> List[int]

def fetch_items(url, category_id, offset, limit):
    params = {'limit': limit, 'offset': offset, 'sort_type': 'popular', 'category_id': category_id}
    return fetch_data(url, params)

def write_items_to_csv(url, category_ids):
    with open('countries.csv', 'w', encoding='UTF8') as f:
        writer = csv.writer(f)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for category_id in category_ids:
                offset = 0
                limit = 200
                params = {'limit': 1, 'offset': offset, 'sort_type': 'popular', 'category_id': category_id}
                try:
                    initial_response = fetch_data(url, params)
                except RetryError:
                    print(f"Не удалось получить начальные данные для категории {category_id} после повторных попыток.")
                    continue

                max_count = initial_response.get('metadata', {}).get('count', 0)
                if limit > max_count:
                    limit = max_count

                while max_count > offset:
                    futures.append(executor.submit(fetch_items, url, category_id, offset, limit))
                    offset += limit
                    if max_count - offset < limit:
                        limit = max_count - offset

            for future in as_completed(futures):
                try:
                    data = future.result()
                except RetryError:
                    print("Не удалось получить данные о товарах после повторных попыток.")
                    continue

                products = data.get('products', [])
                rows = ((TOWN,
                         offer['code'], item['name'],
                         offer['retail_price'], offer['discount_price'],
                         offer['is_available']) for item in products for offer in item['offers'])
                for row in rows:
                    writer.writerow(row)

def main():
    if not CATEGORY_ID:
        category_ids = get_category_list(url=CATEGORIES_URL)[:2]
        write_items_to_csv(url=ITEMS_URL, category_ids=category_ids)
    else:
        write_items_to_csv(url=ITEMS_URL, category_ids=(CATEGORY_ID,))

if __name__ == '__main__':
    main()