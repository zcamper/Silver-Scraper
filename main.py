import asyncio
import json
import re
from datetime import datetime, timezone
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

from apify import Actor
from bs4 import BeautifulSoup
from curl_cffi.requests import Session

SITE_HOST = 'www.silver.com'
SITE_HOSTS = {'silver.com', 'www.silver.com'}
BASE_URL = 'https://www.silver.com'
SEARCH_URL_TEMPLATE = 'https://www.silver.com/?s={query}&post_type=product'

# SearchSpring API (Silver.com uses SearchSpring for search)
SEARCHSPRING_SITE_ID = 'ey66qs'
SEARCHSPRING_SEARCH_URL = 'https://api.searchspring.net/api/search/search.json'

AVAILABILITY_STATES = ['In Stock', 'Out of Stock', 'Pre-Order', 'Sold Out', 'Coming Soon', 'Discontinued']
MAX_DESCRIPTION_LENGTH = 2000
SKIP_PATH_SEGMENTS = ['/about/', '/contact/', '/faq/', '/help/', '/blog/', '/my-account/', '/cart/', '/checkout/', '/shipping/', '/privacy/', '/terms/', '/wp-admin/', '/wp-content/']

products_scraped = 0
scraped_urls: set[str] = set()


def parse_price(price_str: str) -> float | None:
    if not price_str:
        return None
    match = re.search(r'\$?([\d,]+\.?\d*)', price_str)
    if match:
        try:
            return float(match.group(1).replace(',', ''))
        except ValueError:
            return None
    return None


def validate_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ''
        return parsed.scheme in ('http', 'https') and host in SITE_HOSTS
    except Exception:
        return False


def is_search_url(url: str) -> bool:
    return '?s=' in url or '&s=' in url or '/search' in url


def is_category_url(url: str) -> bool:
    if is_search_url(url):
        return False
    parsed = urlparse(url)
    path = parsed.path.lower().strip('/')
    if not path:
        return True
    top_categories = ('silver-bullion', 'gold-bullion', 'platinum-bullion', 'palladium-bullion',
                      'silver-coins', 'gold-coins', 'silver-bars', 'gold-bars',
                      'silver-rounds', 'product-category')
    segments = [s for s in path.split('/') if s]
    if segments and segments[0] in top_categories:
        return True
    if 'product-category' in path:
        return True
    if 'page' in segments:
        return True
    return False


def is_product_url(url: str) -> bool:
    if not validate_url(url):
        return False
    if is_search_url(url):
        return False
    if any(skip in url for skip in SKIP_PATH_SEGMENTS):
        return False
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    if not path:
        return False
    if is_category_url(url):
        return False
    # Silver.com products: /{product-slug}/ (single segment path)
    segments = [s for s in path.split('/') if s]
    if len(segments) == 1 and '.' not in segments[0]:
        return True
    return False


def search_searchspring(http: Session, query: str, proxies: dict, results_per_page: int = 48, page: int = 1) -> list[dict]:
    """Query the SearchSpring API to get Silver.com search results."""
    params = {
        'siteId': SEARCHSPRING_SITE_ID,
        'q': query,
        'resultsPerPage': str(results_per_page),
        'page': str(page),
    }
    try:
        resp = http.get(SEARCHSPRING_SEARCH_URL, params=params, proxies=proxies, timeout=30)
        if resp.status_code != 200:
            Actor.log.warning(f"SearchSpring API returned {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        Actor.log.error(f"SearchSpring API request failed: {e}")
        return []

    total = data.get('pagination', {}).get('totalResults', 0)
    raw_results = data.get('results', [])
    Actor.log.info(f"SearchSpring API: {total} total results, got {len(raw_results)} (page {page})")

    products = []
    for r in raw_results:
        url = r.get('url', '') or r.get('product_url', '')
        if url and not url.startswith('http'):
            url = f"{BASE_URL}{url}" if url.startswith('/') else f"{BASE_URL}/{url}"

        name = r.get('name', '') or r.get('title', '')
        price_val = r.get('price', None) or r.get('sale_price', None)
        price_text = f"${float(price_val):,.2f}" if price_val else None

        image = r.get('thumbnailImageUrl', '') or r.get('imageUrl', '') or r.get('image', '')
        if image and not image.startswith('http'):
            image = f"{BASE_URL}{image}"

        sku = r.get('sku', '') or r.get('uid', '')

        description = r.get('description', '') or r.get('short_description', '')
        if isinstance(description, list):
            description = description[0] if description else ''
        if description:
            # Strip HTML tags from description
            description = BeautifulSoup(description, 'html.parser').get_text(strip=True)
            description = description[:MAX_DESCRIPTION_LENGTH]

        availability = 'In Stock'
        stock = r.get('ss_in_stock', '') or r.get('in_stock', '') or r.get('availability', '')
        if isinstance(stock, bool):
            availability = 'In Stock' if stock else 'Out of Stock'
        elif isinstance(stock, str) and stock:
            if stock.lower() in ('0', 'false', 'no', 'out'):
                availability = 'Out of Stock'

        if url and name:
            products.append({
                'url': url,
                'name': name,
                'price': price_text,
                'priceNumeric': float(price_val) if price_val else None,
                'image': image or None,
                'sku': str(sku) if sku else None,
                'description': description or None,
                'availability': availability,
            })

    return products


def extract_listing_products(html: str, base_url: str) -> list[dict]:
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    seen = set()

    for item in soup.select('.product, .type-product, .products .product'):
        link_el = item.select_one('a[href]')
        if not link_el:
            continue

        url = urljoin(base_url, link_el.get('href', ''))
        if url in seen or not validate_url(url):
            continue
        seen.add(url)

        name_el = item.select_one('.woocommerce-loop-product__title, h2, h3')
        name = name_el.get_text(strip=True) if name_el else link_el.get_text(strip=True)

        price_el = item.select_one('.price .woocommerce-Price-amount, .price .amount, .price')
        price = price_el.get_text(strip=True) if price_el else None
        if price and price.count('$') > 1:
            prices = re.findall(r'\$[\d,]+\.?\d*', price)
            price = prices[-1] if prices else price

        img_el = item.select_one('img')
        image = img_el.get('src') or img_el.get('data-src') if img_el else None

        if name and len(name) > 3:
            products.append({'url': url, 'name': name, 'price': price, 'image': image})

    return products


def extract_product_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')

    # Name
    h1 = soup.select_one('h1')
    name = h1.get_text(strip=True) if h1 else None

    # Price — try itemprop first, then WooCommerce selectors
    price_text = None
    price_numeric = None

    meta_price = soup.select_one('meta[itemprop="price"], meta[property="product:price:amount"]')
    if meta_price:
        content = meta_price.get('content', '')
        if content:
            try:
                price_numeric = float(content)
                price_text = f"${price_numeric:,.2f}"
            except ValueError:
                pass

    if not price_text:
        price_el = soup.select_one(
            '[itemprop="price"], .woocommerce-Price-amount, '
            '.price ins .amount, .price .amount, .summary .price'
        )
        if price_el:
            content = price_el.get('content')
            if content:
                try:
                    price_numeric = float(content)
                    price_text = f"${price_numeric:,.2f}"
                except ValueError:
                    pass
            if not price_text:
                price_text = price_el.get_text(strip=True)
                if price_text and price_text.count('$') > 1:
                    prices = re.findall(r'\$[\d,]+\.?\d*', price_text)
                    price_text = prices[-1] if prices else price_text
                price_numeric = parse_price(price_text)

    # Image
    og_image = soup.select_one('meta[property="og:image"]')
    image_url = og_image.get('content') if og_image else None
    if not image_url:
        img_el = soup.select_one('.woocommerce-product-gallery img, img.wp-post-image')
        image_url = img_el.get('src') if img_el else None

    # SKU
    sku = None
    sku_el = soup.select_one('[itemprop="sku"], .sku')
    if sku_el:
        sku = sku_el.get('content') or sku_el.get_text(strip=True)

    # Availability
    availability = "Unknown"
    avail_el = soup.select_one('[itemprop="availability"]')
    if avail_el:
        avail_text = avail_el.get('content', '') or avail_el.get('href', '') or avail_el.get_text()
        if 'InStock' in avail_text:
            availability = "In Stock"
        elif 'OutOfStock' in avail_text:
            availability = "Out of Stock"
        elif 'PreOrder' in avail_text:
            availability = "Pre-Order"

    if availability == "Unknown":
        page_text = soup.get_text()
        for state in AVAILABILITY_STATES:
            if state in page_text:
                availability = state
                break

    # Description
    desc_el = soup.select_one(
        '.woocommerce-product-details__short-description, '
        '[itemprop="description"], .product-short-description'
    )
    description = desc_el.get_text(strip=True)[:MAX_DESCRIPTION_LENGTH] if desc_el else None

    return {
        'name': name,
        'price': price_text if price_text and '$' in str(price_text) else None,
        'priceNumeric': price_numeric if price_numeric else parse_price(price_text) if price_text else None,
        'imageUrl': image_url,
        'sku': sku,
        'availability': availability,
        'description': description,
    }


def get_next_page_url(html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html, 'html.parser')
    next_link = soup.select_one('.woocommerce-pagination a.next, a.next.page-numbers, .pagination a.next')
    if next_link:
        return urljoin(base_url, next_link.get('href', ''))
    return None


def init_session(proxies: dict) -> Session:
    http = Session(impersonate="chrome110")
    home_resp = http.get(f"{BASE_URL}/", proxies=proxies, timeout=30)
    Actor.log.info(f"Homepage warm-up: status={home_resp.status_code}, cookies={len(http.cookies)}")
    if home_resp.status_code != 200:
        Actor.log.warning(f"Homepage returned {home_resp.status_code}, scraping may fail")
    http.headers.update({'Referer': f'{BASE_URL}/'})
    return http


async def scrape_search(http: Session, query: str, proxies: dict, max_items: int) -> None:
    """Scrape search results using the SearchSpring API."""
    global products_scraped
    page = 1
    results_per_page = min(48, max_items - products_scraped)

    while products_scraped < max_items:
        products = search_searchspring(http, query, proxies, results_per_page=results_per_page, page=page)
        if not products:
            break

        for product in products:
            if products_scraped >= max_items:
                break

            prod_url = product['url'].rstrip('/')
            if prod_url in scraped_urls:
                continue
            scraped_urls.add(prod_url)

            # Fetch full product page for detailed data
            try:
                prod_resp = http.get(prod_url, proxies=proxies, timeout=30)
                if prod_resp.status_code == 200:
                    details = extract_product_details(prod_resp.text)
                    await Actor.push_data({
                        'url': prod_url,
                        'name': details['name'] or product.get('name', ''),
                        'price': details['price'] or product.get('price'),
                        'priceNumeric': details['priceNumeric'] or product.get('priceNumeric'),
                        'imageUrl': details['imageUrl'] or product.get('image'),
                        'sku': details['sku'] or product.get('sku'),
                        'availability': details['availability'],
                        'description': details['description'] or product.get('description'),
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    Actor.log.warning(f"Product page {prod_url} returned {prod_resp.status_code}, using API data")
                    await Actor.push_data({
                        'url': prod_url,
                        'name': product.get('name', ''),
                        'price': product.get('price'),
                        'priceNumeric': product.get('priceNumeric'),
                        'imageUrl': product.get('image'),
                        'sku': product.get('sku'),
                        'availability': product.get('availability', 'In Stock'),
                        'description': product.get('description'),
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
            except Exception as e:
                Actor.log.warning(f"Failed to fetch product {prod_url}: {e}, using API data")
                await Actor.push_data({
                    'url': prod_url,
                    'name': product.get('name', ''),
                    'price': product.get('price'),
                    'priceNumeric': product.get('priceNumeric'),
                    'imageUrl': product.get('image'),
                    'sku': product.get('sku'),
                    'availability': product.get('availability', 'In Stock'),
                    'description': product.get('description'),
                    'scrapedAt': datetime.now(timezone.utc).isoformat(),
                })

            products_scraped += 1
            Actor.log.info(f"Scraped {products_scraped}/{max_items} products")

        page += 1
        results_per_page = min(48, max_items - products_scraped)


async def scrape_listing(http: Session, url: str, proxies: dict, max_items: int) -> None:
    global products_scraped
    page_num = 1
    current_url = url

    while current_url and products_scraped < max_items:
        Actor.log.info(f"Fetching listing page {page_num}: {current_url}")
        try:
            response = http.get(current_url, proxies=proxies, timeout=30)
        except Exception as e:
            Actor.log.error(f"Failed to fetch listing {current_url}: {e}")
            break

        if response.status_code != 200:
            Actor.log.warning(f"Non-200 status ({response.status_code}) for listing {current_url}")
            break

        products = extract_listing_products(response.text, current_url)
        Actor.log.info(f"Found {len(products)} products on listing page {page_num}")

        for product in products:
            if products_scraped >= max_items:
                break

            prod_url = product['url'].rstrip('/')
            if prod_url in scraped_urls:
                continue
            scraped_urls.add(prod_url)

            try:
                prod_resp = http.get(prod_url, proxies=proxies, timeout=30)
                if prod_resp.status_code == 200:
                    details = extract_product_details(prod_resp.text)
                    await Actor.push_data({
                        'url': prod_url,
                        'name': details['name'] or product.get('name', ''),
                        'price': details['price'] or product.get('price'),
                        'priceNumeric': details['priceNumeric'] or parse_price(product.get('price')),
                        'imageUrl': details['imageUrl'] or product.get('image'),
                        'sku': details['sku'],
                        'availability': details['availability'],
                        'description': details['description'],
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
                else:
                    Actor.log.warning(f"Product page {prod_url} returned {prod_resp.status_code}")
                    await Actor.push_data({
                        'url': prod_url,
                        'name': product.get('name', ''),
                        'price': product.get('price'),
                        'priceNumeric': parse_price(product.get('price')),
                        'imageUrl': product.get('image'),
                        'sku': None,
                        'availability': 'Unknown',
                        'description': None,
                        'scrapedAt': datetime.now(timezone.utc).isoformat(),
                    })
            except Exception as e:
                Actor.log.warning(f"Failed to fetch product {prod_url}: {e}")

            products_scraped += 1
            Actor.log.info(f"Scraped {products_scraped}/{max_items} products")

        next_url = get_next_page_url(response.text, current_url)
        if next_url and next_url != current_url:
            current_url = next_url
            page_num += 1
        else:
            break


async def scrape_product(http: Session, url: str, proxies: dict, max_items: int) -> None:
    global products_scraped
    if products_scraped >= max_items:
        return

    url = url.rstrip('/')
    if url in scraped_urls:
        return
    scraped_urls.add(url)

    Actor.log.info(f"Fetching product ({products_scraped + 1}/{max_items}): {url}")
    try:
        response = http.get(url, proxies=proxies, timeout=30)
    except Exception as e:
        Actor.log.error(f"Failed to fetch product {url}: {e}")
        return

    if response.status_code != 200:
        Actor.log.warning(f"Non-200 status ({response.status_code}) for product {url}")
        return

    details = extract_product_details(response.text)
    await Actor.push_data({
        'url': url,
        'name': details['name'],
        'price': details['price'],
        'priceNumeric': details['priceNumeric'],
        'imageUrl': details['imageUrl'],
        'sku': details['sku'],
        'availability': details['availability'],
        'description': details['description'],
        'scrapedAt': datetime.now(timezone.utc).isoformat(),
    })

    products_scraped += 1
    Actor.log.info(f"Scraped {products_scraped}/{max_items} products")


async def main():
    global products_scraped

    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls_input = actor_input.get("start_urls", [])
        search_terms = actor_input.get("search_terms", [])
        max_items = actor_input.get("max_items", 10)

        # Build search queries and start URLs
        search_queries = []
        start_urls = []
        for term in search_terms:
            term = term.strip()
            if term:
                search_queries.append(term)
                Actor.log.info(f"Added search term: '{term}' (will use SearchSpring API)")

        for item in start_urls_input:
            if isinstance(item, dict) and "url" in item:
                url = item["url"]
            elif isinstance(item, str):
                url = item
            else:
                continue
            if validate_url(url):
                start_urls.append(url)
            else:
                Actor.log.warning(f"Skipping non-Silver.com URL: {url}")

        if not search_queries and not start_urls:
            default_term = "Silver coin"
            search_queries = [default_term]
            Actor.log.info(f"No input provided, defaulting to search: '{default_term}'")

        Actor.log.info(f"Starting Silver.com Scraper with {len(search_queries)} search queries, {len(start_urls)} start URLs, max_items={max_items}")

        Actor.log.info("Configuring RESIDENTIAL proxy with US country")
        proxy_configuration = await Actor.create_proxy_configuration(
            actor_proxy_input={
                'useApifyProxy': True,
                'apifyProxyGroups': ['RESIDENTIAL'],
                'apifyProxyCountry': 'US',
            },
        )

        proxy_url = await proxy_configuration.new_url()
        proxies = {"http": proxy_url, "https": proxy_url}

        http = init_session(proxies)

        # Process search queries via SearchSpring API
        for query in search_queries:
            if products_scraped >= max_items:
                break
            Actor.log.info(f"Using SearchSpring API for query: '{query}'")
            await scrape_search(http, query, proxies, max_items)

        # Process start URLs
        for url in start_urls:
            if products_scraped >= max_items:
                break

            if is_search_url(url):
                parsed = urlparse(url)
                qs = parse_qs(parsed.query)
                query = qs.get('s', qs.get('q', ['']))[0]
                if query:
                    Actor.log.info(f"Using SearchSpring API for search URL query: '{query}'")
                    await scrape_search(http, query, proxies, max_items)
            elif is_category_url(url):
                await scrape_listing(http, url, proxies, max_items)
            elif is_product_url(url):
                await scrape_product(http, url, proxies, max_items)
            else:
                Actor.log.warning(f"Could not classify URL, trying as listing: {url}")
                await scrape_listing(http, url, proxies, max_items)

        Actor.log.info(f'Scraping completed. Total products scraped: {products_scraped}')


if __name__ == "__main__":
    asyncio.run(main())
