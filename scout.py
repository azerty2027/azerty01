"""
VINYL SCOUT v10
- Phase 1 : scraping sources experts (Kiswell, DD, Superfly, Diaspora, SOFA Records)
- Phase 3 : recherche opportunites Leboncoin (API JSON) + Vinted (ScrapeOps) + cdandlp (ScrapeOps) + eBay API
  → rotation batch : 100 disques/jour max, tournant sur last_searched dans vinyl_db.json
"""

import requests
from bs4 import BeautifulSoup
import json, re, os, time, urllib.parse
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
MIN_PRICE = 100
MAX_PRICE_RATIO = 0.40
DB_FILE = "vinyl_db.json"
ALERT_FILE = "ALERTES.md"
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
SCRAPEOPS_KEY = os.environ.get("SCRAPEOPS_KEY", "")
BATCH_SIZE = 100


# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

STOPWORDS = {
    'records', 'record', 'company', 'label', 'edition', 'editions',
    'productions', 'production', 'international', 'pressing', 'reissue',
    'publishing', 'release', 'distributed', 'distribution',
    'vinyl', 'stereo', 'mono', 'disc', 'disk', 'album', 'single',
    'original', 'limited', 'volume', 'studio', 'live',
    'music', 'musique', 'orchestra', 'orchestre', 'ensemble', 'band',
    'trio', 'quartet', 'quintet', 'sextet', 'session', 'suite',
    'theme', 'themes', 'song', 'songs', 'dance', 'plays', 'featuring',
    'present', 'presents', 'various', 'artists', 'compilation',
    'collection', 'series', 'best', 'great', 'super', 'special',
    'jazz', 'blues', 'soul', 'funk', 'disco', 'rock', 'folk',
    'latin', 'afro', 'reggae', 'bossa', 'nova', 'swing',
    'avec', 'dans', 'pour', 'from', 'the', 'and', 'feat',
    'chant', 'monde', 'club', 'libre',
    'france', 'french', 'italy', 'italian', 'germany', 'german',
    'sweden', 'swedish', 'japan', 'japanese', 'brasil', 'brazil',
    'belgium', 'swiss', 'spain', 'spanish', 'greece', 'greek',
    'africa', 'african', 'india', 'indian', 'lebanese',
    'columbia', 'ducretet', 'thomson', 'polydor', 'barclay',
    'philips', 'atlantic', 'verve',
}


def extract_price(text):
    if not text:
        return None
    text = str(text).replace('\xa0', '').replace('\u202f', '').replace(',', '.').replace(' ', '')
    match = re.search(r'(\d{2,4}\.?\d*)\s*[€£]', text)
    if match:
        return float(match.group(1))
    match = re.search(r'(\d{2,4}\.?\d*)', text)
    return float(match.group(1)) if match else None


def clean_title(title):
    title = re.sub(r'[^\w\s]', ' ', title)
    words = [w for w in title.split() if len(w) > 2]
    return ' '.join(words[:5])


def words_from(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    return set(
        w for w in text.split()
        if len(w) > 3 and not w.isdigit() and w not in STOPWORDS
    )


def parse_artist_album(title):
    title_clean = re.sub(r'\(.*?\)', '', title).strip()
    parts = re.split(r'\s[—–\-]\s', title_clean, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return '', title_clean.strip()


# ─────────────────────────────────────────────
# PHASE 1 — SCRAPING SOURCES EXPERTS
# ─────────────────────────────────────────────

def scrape_victorkiswell():
    results = {}
    categories = [
        'afro', 'caribbean', 'arabic-persian', 'bollywood', 'latin',
        'funk', 'jazz', 'library-music', 'soundtracks', 'early-electro',
        'electro-cosmic', 'pop-jerk', 'psych-prog', 'sitar-bangers',
        'breaks-loops', 'we-once-had-it'
    ]
    for cat in categories:
        page = 1
        while page <= 20:
            try:
                url = f"http://www.victorkiswell.com/v3/?product_cat={cat}&paged={page}"
                r = requests.get(url, headers=HEADERS, timeout=25)
                soup = BeautifulSoup(r.text, 'html.parser')
                items = soup.select('li.product')
                if not items:
                    break
                for item in items:
                    title_el = item.select_one('.woocommerce-loop-product__title') or item.select_one('h2')
                    price_el = item.select_one('.price')
                    link_el = item.select_one('a.woocommerce-LoopProduct-link') or item.select_one('a')
                    if not title_el or not link_el:
                        continue
                    price = extract_price(price_el.get_text() if price_el else '')
                    if not price or price < MIN_PRICE:
                        continue
                    url_item = link_el['href']
                    if url_item in results:
                        continue
                    sold = any(x in item.get_text().lower() for x in ['out of stock', 'sold', 'epuise'])
                    results[url_item] = {
                        'source': 'Victor Kiswell',
                        'title': title_el.get_text(strip=True),
                        'price_ref': price,
                        'url': url_item,
                        'sold': sold
                    }
                page += 1
                time.sleep(1.5)
            except Exception as e:
                print(f"VK {cat} page {page}: {e}")
                break
    print(f"Victor Kiswell: {len(results)} disques")
    return list(results.values())


def scrape_diggersdigest():
    results = {}
    page = 1
    while page <= 30:
        try:
            url = f"https://www.diggersdigest.com/products?page={page}"
            r = requests.get(url, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('a.prod-thumb')
            if not items:
                break
            found_new = False
            for item in items:
                title_el = item.select_one('.prod-thumb-name')
                price_el = item.select_one('span[data-currency-amount]')
                sold_el = item.select_one('.prod-thumb-status')
                if not title_el:
                    continue
                href = item.get('href', '')
                url_item = href if href.startswith('http') else 'https://www.diggersdigest.com' + href
                if url_item in results:
                    continue
                sold = sold_el is not None and 'sold' in sold_el.get_text().lower()
                if not price_el:
                    continue
                price = float(price_el.get('data-currency-amount', 0))
                if price < MIN_PRICE:
                    continue
                results[url_item] = {
                    'source': "Digger's Digest",
                    'title': title_el.get_text(strip=True),
                    'price_ref': price,
                    'url': url_item,
                    'sold': sold
                }
                found_new = True
            if not found_new:
                break
            page += 1
            time.sleep(1.5)
        except Exception as e:
            print(f"DD page {page}: {e}")
            break
    print(f"Digger's Digest: {len(results)} disques")
    return list(results.values())


def scrape_superfly():
    results = {}
    categories = [
        ('soul-funk-disco',     '99000179'),
        ('jazz',                '99000181'),
        ('afro',                '99000182'),
        ('latin',               '99000187'),
        ('brasil',              '99000183'),
        ('european',            '99000185'),
        ('reggae',              '99000184'),
        ('new-grooves-hip-hop', '99000191'),
        ('groovy-rock',         '99000190'),
    ]
    for cat_name, cat_id in categories:
        page = 1
        while page <= 20:
            try:
                url = f"https://www.superflyrecords.com/listing/2/0-{cat_id}-0/0_{page}/superflyrecords-{cat_name}.html"
                r = requests.get(url, headers=HEADERS, timeout=25)
                if r.status_code == 404:
                    break
                soup = BeautifulSoup(r.text, 'html.parser')
                rows = soup.select('tr')
                found_new = False
                for row in rows:
                    links = row.select('a[href*="/item/"]')
                    if not links:
                        continue
                    href = links[0].get('href', '')
                    url_item = href if href.startswith('http') else 'https://www.superflyrecords.com' + href
                    if url_item in results:
                        continue
                    parts = [l.get_text(strip=True) for l in links if l.get_text(strip=True)]
                    title = ' — '.join(parts) if parts else ''
                    if not title:
                        continue
                    price = extract_price(row.get_text())
                    if not price or price < MIN_PRICE:
                        continue
                    sold = 'sold' in row.get_text().lower() or 'vendu' in row.get_text().lower()
                    results[url_item] = {
                        'source': 'Superfly Records',
                        'title': title,
                        'price_ref': price,
                        'url': url_item,
                        'sold': sold
                    }
                    found_new = True
                if not found_new:
                    seen_urls = set()
                    for link in soup.select('a[href*="/item/"]'):
                        href = link.get('href', '')
                        if not href or href in seen_urls:
                            continue
                        seen_urls.add(href)
                        url_item = href if href.startswith('http') else 'https://www.superflyrecords.com' + href
                        if url_item in results:
                            continue
                        block = link.find_parent(['div', 'li', 'td', 'article'])
                        if not block:
                            continue
                        title_parts = [
                            a.get_text(strip=True)
                            for a in block.select('a[href*="/item/"]')
                            if a.get_text(strip=True)
                        ]
                        title = ' — '.join(title_parts) if title_parts else link.get_text(strip=True)
                        if not title:
                            continue
                        price = extract_price(block.get_text())
                        if not price or price < MIN_PRICE:
                            continue
                        sold = 'sold' in block.get_text().lower() or 'vendu' in block.get_text().lower()
                        results[url_item] = {
                            'source': 'Superfly Records',
                            'title': title,
                            'price_ref': price,
                            'url': url_item,
                            'sold': sold
                        }
                        found_new = True
                if not found_new:
                    break
                page += 1
                time.sleep(1.5)
            except Exception as e:
                print(f"Superfly {cat_name} page {page}: {e}")
                break
    print(f"Superfly Records: {len(results)} disques")
    return list(results.values())


def scrape_diaspora():
    results = {}
    page = 0
    while page <= 100:
        try:
            url = f"https://www.diasporarecords.com/search?page={page}"
            r = requests.get(url, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r.text, 'html.parser')
            blocks = soup.select('article') or soup.select('.views-row') or soup.select('[class*="record"]')
            if not blocks:
                blocks = [a.find_parent(['div', 'li']) for a in soup.select(
                    'a[href*="/jazz"], a[href*="/afro"], a[href*="/soul"], a[href*="/africa"], '
                    'a[href*="/caribbean"], a[href*="/latin"], a[href*="/brasil"]'
                ) if a.find_parent(['div', 'li'])]
            found_new = False
            seen = set()
            for block in blocks:
                if not block:
                    continue
                link_el = block.select_one('a')
                if not link_el:
                    continue
                href = link_el.get('href', '')
                if not href or href in seen:
                    continue
                seen.add(href)
                full_url = 'https://www.diasporarecords.com' + href if href.startswith('/') else href
                if 'diasporarecords.com' not in full_url or full_url in results:
                    continue
                text = block.get_text(separator=' ')
                price_match = re.search(r'(\d+[,.]?\d*)\s*€', text)
                if not price_match:
                    continue
                price = extract_price(price_match.group())
                if not price or price < MIN_PRICE:
                    continue
                title = link_el.get_text(strip=True)
                if not title:
                    continue
                sold = any(x in text.lower() for x in ['sold out', 'unavailable', 'vendu'])
                results[full_url] = {
                    'source': 'Diaspora Records',
                    'title': title,
                    'price_ref': price,
                    'url': full_url,
                    'sold': sold
                }
                found_new = True
            if not found_new:
                break
            page += 1
            time.sleep(1.5)
        except Exception as e:
            print(f"Diaspora page {page}: {e}")
            break
    print(f"Diaspora Records: {len(results)} disques")
    return list(results.values())


def scrape_sofarecords():
    results = {}
    MIN_PRICE_SOFA = 70
    categories = [
        ('afro-funk-afro-disco',        'c98000508'),
        ('dj-stuff-club-electronics',   'c98000509'),
        ('afro-beat',                   'c98000510'),
        ('highlife',                    'c98000511'),
        ('mali-guinea-senegal',         'c98000489'),
        ('congo-rumba-sukus',           'c98000512'),
        ('ethiopia-sudan-somalia',      'c98000493'),
        ('kenya-east-africa',           'c98000539'),
        ('maghreb-vinyl',               'c98000515'),
        ('lebanon-egypt',               'c98000517'),
        ('french-west-indies-zou',      'c98000519'),
        ('reggae-dub-ska',              'c98000521'),
        ('latin-salsa-boogaloo',        'c98000523'),
        ('brazilian-soul-funk-samba',   'c98000526'),
        ('bossa-nova-brazilian-jazz',   'c98000527'),
        ('japanese-jazz-funk-ambient-city-pop', 'c98000529'),
        ('soul-jazz-jazz-funk',         'c98000535'),
        ('free-jazz-avant-garde',       'c98000537'),
        ('french-jazz-avant-garde',     'c98000531'),
        ('french-grooves-songs',        'c98000532'),
    ]

    def extract_sofa_title(link_el, parent):
        for tag in ['h2', 'h3', 'h4', '[class*="title"]', '[class*="name"]']:
            el = parent.select_one(tag)
            if el and el != link_el and el.get_text(strip=True):
                return el.get_text(strip=True)
        children = [
            c.get_text(strip=True)
            for c in link_el.find_all(['span', 'div', 'p', 'em', 'strong'], recursive=False)
            if c.get_text(strip=True)
        ]
        if len(children) >= 2:
            return ' — '.join(children[:2])
        if children:
            return children[0]
        from bs4 import NavigableString
        direct_texts = [
            str(t).strip()
            for t in link_el.children
            if isinstance(t, NavigableString) and str(t).strip()
        ]
        if len(direct_texts) >= 2:
            return ' — '.join(direct_texts[:2])
        return link_el.get_text(strip=True)

    for cat_name, cat_id in categories:
        page = 1
        while page <= 20:
            try:
                url = f"https://www.sofarecords.fr/fr/{cat_name}/{cat_id}/"
                if page > 1:
                    url = f"https://www.sofarecords.fr/fr/{cat_name}/{cat_id}/{page}/"
                r = requests.get(url, headers=HEADERS, timeout=25)
                if r.status_code == 404:
                    break
                soup = BeautifulSoup(r.text, 'html.parser')
                items = (soup.select('div.product-info')
                         or soup.select('li.product')
                         or soup.select('[class*="product"]'))
                if not items:
                    links = soup.select('a[href*="/fr/"][href*="/p"]')
                    if not links:
                        break
                    found_new = False
                    for link in links:
                        href = link.get('href', '')
                        if not href or '/p' not in href:
                            continue
                        url_item = href if href.startswith('http') else 'https://www.sofarecords.fr' + href
                        if url_item in results:
                            continue
                        parent = link.find_parent(['div', 'li', 'article'])
                        if not parent:
                            continue
                        price = extract_price(parent.get_text())
                        if not price or price < MIN_PRICE_SOFA:
                            continue
                        title = extract_sofa_title(link, parent)
                        if not title:
                            continue
                        sold = any(x in parent.get_text().lower() for x in ['sold', 'vendu', 'épuisé'])
                        results[url_item] = {
                            'source': 'SOFA Records',
                            'title': title,
                            'price_ref': price,
                            'url': url_item,
                            'sold': sold
                        }
                        found_new = True
                    if not found_new:
                        break
                else:
                    found_new = False
                    for item in items:
                        link_el = item.select_one('a')
                        if not link_el:
                            continue
                        href = link_el.get('href', '')
                        url_item = href if href.startswith('http') else 'https://www.sofarecords.fr' + href
                        if url_item in results:
                            continue
                        price = extract_price(item.get_text())
                        if not price or price < MIN_PRICE_SOFA:
                            continue
                        title = extract_sofa_title(link_el, item)
                        if not title:
                            continue
                        sold = any(x in item.get_text().lower() for x in ['sold', 'vendu', 'épuisé'])
                        results[url_item] = {
                            'source': 'SOFA Records',
                            'title': title,
                            'price_ref': price,
                            'url': url_item,
                            'sold': sold
                        }
                        found_new = True
                    if not found_new:
                        break
                page += 1
                time.sleep(1.5)
            except Exception as e:
                print(f"SOFA {cat_name} page {page}: {e}")
                break
    print(f"SOFA Records: {len(results)} disques")
    return list(results.values())


# ─────────────────────────────────────────────
# PHASE 3 — ROTATION BATCH
# ─────────────────────────────────────────────

def select_batch(actifs, db, batch_size=BATCH_SIZE):
    def sort_key(r):
        entry = db.get(r['url'], {})
        last = entry.get('last_searched')
        return last if last else '0000-00-00'
    sorted_actifs = sorted(actifs, key=sort_key)
    batch = sorted_actifs[:batch_size]
    total = len(actifs)
    coverage_days = max(1, -(-total // batch_size))
    print(f"Batch : {len(batch)}/{total} disques actifs → couverture complète en ~{coverage_days} jours")
    return batch


def mark_searched(db, records):
    now = datetime.now().isoformat()
    for r in records:
        if r['url'] in db:
            db[r['url']]['last_searched'] = now


# ─────────────────────────────────────────────
# PHASE 3 — RECHERCHE MARCHES
# ─────────────────────────────────────────────

def scrapeops_get(url):
    return requests.get(
        "https://proxy.scrapeops.io/v1/",
        params={
            "api_key": SCRAPEOPS_KEY,
            "url": url,
            "residential": "true",
            "country": "fr",
        },
        timeout=90
    )


def search_leboncoin(title, max_price):
    """
    Recherche LBC via ScrapeOps proxy résidentiel + render_js.
    Catégorie 34 = CD, Vinyles, Cassettes.
    """
    if not SCRAPEOPS_KEY:
        return []
    results = []
    query = urllib.parse.quote(clean_title(title))
    try:
        url = f"https://www.leboncoin.fr/recherche?text={query}&category=34&price=min-{int(max_price)}"
        r = requests.get(
            "https://proxy.scrapeops.io/v1/",
            params={
                "api_key": SCRAPEOPS_KEY,
                "url": url,
                "residential": "true",
                "country": "fr",
                "render_js": "true",
            },
            timeout=90
        )
        print(f"  LBC status: {r.status_code} | query: {clean_title(title)} | max: {max_price}€")
        if r.status_code != 200:
            print(f"  LBC erreur: {r.text[:200]}")
            return results
        soup = BeautifulSoup(r.text, 'html.parser')
        # LBC charge les annonces dans des balises <a> avec data-qa-id
        ads = (soup.select('a[data-qa-id="aditem_container"]')
               or soup.select('[data-test-id="ad"]')
               or soup.select('a[href*="/ad/"]'))
        print(f"  LBC annonces: {len(ads)}")
        for ad in ads[:20]:
            title_el = (ad.select_one('[data-qa-id="aditem_title"]')
                        or ad.select_one('p[class*="title"]')
                        or ad.select_one('h2'))
            price_el = (ad.select_one('[data-qa-id="aditem_price"]')
                        or ad.select_one('[class*="price"]'))
            href = ad.get('href', '')
            if not href:
                continue
            ad_url = href if href.startswith('http') else 'https://www.leboncoin.fr' + href
            price = extract_price(price_el.get_text()) if price_el else None
            if not price or price > max_price:
                continue
            results.append({
                "platform": "leboncoin.fr",
                "title": title_el.get_text(strip=True) if title_el else '',
                "price": price,
                "url": ad_url
            })
        time.sleep(1)
    except Exception as e:
        print(f"  LBC exception: {e}")
    return results


def search_vinted(title, max_price):
    if not SCRAPEOPS_KEY:
        return []
    results = []
    query = urllib.parse.quote(clean_title(title))
    try:
        url = f"https://www.vinted.fr/catalog?search_text={query}&price_to={int(max_price)}&catalog[]=3041"
        r = scrapeops_get(url)
        print(f"  VTD status: {r.status_code} | query: {clean_title(title)} | max: {max_price}€")
        if r.status_code != 200:
            return results
        soup = BeautifulSoup(r.text, 'html.parser')
        items = (soup.select('[data-testid="grid-item"]')
                 or soup.select('[class*="ItemBox"]')
                 or soup.select('div[class*="item"]'))
        print(f"  VTD articles: {len(items)}")
        for item in items[:20]:
            link_el = item.select_one('a')
            price_el = (item.select_one('[class*="price"]')
                        or item.select_one('[data-testid*="price"]'))
            if not link_el:
                continue
            href = link_el.get('href', '')
            item_url = href if href.startswith('http') else 'https://www.vinted.fr' + href
            price = extract_price(price_el.get_text()) if price_el else None
            if not price or price > max_price:
                continue
            title_el = item.select_one('[class*="title"]') or item.select_one('h3') or link_el
            results.append({
                "platform": "vinted.fr",
                "title": title_el.get_text(strip=True) if title_el else '',
                "price": price,
                "url": item_url
            })
        time.sleep(1)
    except Exception as e:
        print(f"  VTD exception: {e}")
    return results


def search_cdandlp(title, max_price):
    """
    cdandlp.com via ScrapeOps (le site bloque les requêtes directes).
    """
    if not SCRAPEOPS_KEY:
        return []
    results = []
    query = urllib.parse.quote(clean_title(title))
    try:
        url = f"https://www.cdandlp.com/en/search/?q={query}&type=v&price_max={int(max_price)}&c=1"
        r = scrapeops_get(url)
        print(f"  C&LP status: {r.status_code} | query: {clean_title(title)} | max: {max_price}€")
        if r.status_code != 200:
            return results
        soup = BeautifulSoup(r.text, 'html.parser')
        items = (soup.select('article.product')
                 or soup.select('[class*="annonce"]')
                 or soup.select('[class*="product-item"]')
                 or soup.select('li[class*="item"]'))
        print(f"  C&LP items: {len(items)}")
        for item in items[:20]:
            link_el = item.select_one('a')
            price_el = (item.select_one('[class*="price"]')
                        or item.select_one('[itemprop="price"]')
                        or item.select_one('span[class*="prix"]'))
            if not link_el:
                continue
            href = link_el.get('href', '')
            item_url = href if href.startswith('http') else 'https://www.cdandlp.com' + href
            price_text = (price_el.get('content') or price_el.get_text()) if price_el else ''
            price = extract_price(price_text)
            if not price or price > max_price:
                continue
            title_el = (item.select_one('[class*="title"]')
                        or item.select_one('h2') or item.select_one('h3')
                        or link_el)
            results.append({
                "platform": "cdandlp.com",
                "title": title_el.get_text(strip=True) if title_el else '',
                "price": price,
                "url": item_url
            })
        time.sleep(1)
    except Exception as e:
        print(f"  C&LP exception: {e}")
    return results


def search_ebay(title, max_price):
    results = []
    ebay_key = os.environ.get("EBAY_APP_ID", "")
    if not ebay_key:
        return results
    query = clean_title(title)
    try:
        params = {
            "OPERATION-NAME": "findItemsAdvanced",
            "SERVICE-VERSION": "1.0.0",
            "SECURITY-APPNAME": ebay_key,
            "RESPONSE-DATA-FORMAT": "JSON",
            "keywords": query,
            "categoryId": "306",
            "itemFilter(0).name": "MaxPrice",
            "itemFilter(0).value": str(int(max_price)),
            "itemFilter(0).paramName": "Currency",
            "itemFilter(0).paramValue": "EUR",
            "itemFilter(1).name": "ListingType",
            "itemFilter(1).value": "FixedPrice",
            "itemFilter(2).name": "LocatedIn",
            "itemFilter(2).value": "FR",
            "paginationInput.entriesPerPage": "20",
        }
        r = requests.get(
            "https://svcs.ebay.com/services/search/FindingService/v1",
            params=params, timeout=15
        )
        data = r.json()
        items = (data.get("findItemsAdvancedResponse", [{}])[0]
                     .get("searchResult", [{}])[0]
                     .get("item", []))
        for item in items:
            try:
                price = float(item["sellingStatus"][0]["currentPrice"][0]["__value__"])
            except (IndexError, KeyError, ValueError):
                continue
            if not price or price > max_price:
                continue
            try:
                item_url = item["viewItemURL"][0]
                item_title = item["title"][0]
            except IndexError:
                continue
            results.append({
                "platform": "ebay.fr",
                "title": item_title,
                "price": price,
                "url": item_url
            })
        time.sleep(0.5)
    except Exception as e:
        print(f"eBay: {e}")
    return results


# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE) as f:
            return json.load(f)
    return {}


def save_db(db):
    with open(DB_FILE, 'w') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    # ── Phase 1 : scraping sources experts ──────────────────────────────
    print("=" * 60)
    print("Phase 1 : scraping sources experts...")
    all_records = []
    all_records += scrape_victorkiswell()
    all_records += scrape_diggersdigest()
    all_records += scrape_superfly()
    all_records += scrape_diaspora()
    all_records += scrape_sofarecords()

    seen = set()
    unique_records = []
    for r in all_records:
        if r['url'] not in seen:
            seen.add(r['url'])
            unique_records.append(r)
    all_records = unique_records
    print(f"TOTAL: {len(all_records)} disques uniques >= {MIN_PRICE}EUR")

    db = load_db()
    nouveaux_ref = []
    for r in all_records:
        key = r['url']
        if key not in db:
            nouveaux_ref.append(r)
            db[key] = {**r, 'first_seen': datetime.now().isoformat()}
        else:
            db[key]['price_ref'] = r['price_ref']
            db[key]['sold'] = r['sold']
    save_db(db)

    # ── Phase 3 : recherche opportunites marche (batch rotatif) ──────────
    print("\n" + "=" * 60)
    print("Phase 3 : recherche opportunites marche (batch rotatif)...")
    actifs = [r for r in all_records if not r.get('sold')]

    if TEST_MODE:
        print("  [TEST_MODE] Limite a 3 disques pour diagnostic")
        batch = actifs[:3]
    else:
        batch = select_batch(actifs, db, BATCH_SIZE)

    opportunites = []
    for record in batch:
        max_price = round(record['price_ref'] * MAX_PRICE_RATIO, 0)
        found = search_leboncoin(record['title'], max_price)
        found += search_vinted(record['title'], max_price)
        found += search_cdandlp(record['title'], max_price)
        found += search_ebay(record['title'], max_price)
        for f in found:
            marge = record['price_ref'] - f['price']
            ratio = round((marge / record['price_ref']) * 100)
            opportunites.append({
                'ref_title': record['title'],
                'ref_source': record['source'],
                'ref_price': record['price_ref'],
                'found_title': f['title'],
                'found_price': f['price'],
                'found_url': f['url'],
                'platform': f['platform'],
                'marge': marge,
                'marge_pct': ratio,
            })

    mark_searched(db, batch)
    save_db(db)

    seen_urls = set()
    opportunites_uniques = []
    for o in opportunites:
        if o['found_url'] not in seen_urls:
            seen_urls.add(o['found_url'])
            opportunites_uniques.append(o)
    opportunites = sorted(opportunites_uniques, key=lambda x: -x['marge'])

    # ── Rapport ──────────────────────────────────────────────────────────
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"# VINYL SCOUT — Rapport du {now}\n"]
    lines.append(
        f"**Base : {len(db)} disques | Actifs : {len(actifs)} | "
        f"Batch du jour : {len(batch)} | Seuil achat : {int(MAX_PRICE_RATIO*100)}% du prix ref**\n"
    )
    lines.append("---\n")

    if opportunites:
        lines.append(f"## 🔴 {len(opportunites)} OPPORTUNITES MARCHE\n")
        for o in opportunites:
            lines.append(f"### {o['ref_title']}")
            lines.append(f"- **Ref** : {o['ref_price']}€ chez {o['ref_source']}")
            lines.append(f"- **Trouvé** : {o['found_price']}€ sur {o['platform']} — marge potentielle **{o['marge']}€ ({o['marge_pct']}%)**")
            lines.append(f"- [Voir l'annonce]({o['found_url']})")
            lines.append("")
    else:
        lines.append("## ✅ Aucune opportunite marche aujourd'hui\n")

    if nouveaux_ref:
        lines.append(f"---\n## 🆕 {len(nouveaux_ref)} nouveaux en base\n")
        for r in sorted(nouveaux_ref, key=lambda x: -x['price_ref']):
            sold_tag = " ~~[VENDU]~~" if r.get('sold') else ""
            lines.append(f"- **{r['price_ref']}€**{sold_tag} — {r['title']} ({r['source']})")
        lines.append("")

    lines.append("---\n## Catalogue complet\n")
    by_source = {}
    for r in all_records:
        by_source.setdefault(r['source'], []).append(r)
    for source, items in sorted(by_source.items()):
        dispo = [i for i in items if not i.get('sold')]
        vendus = [i for i in items if i.get('sold')]
        lines.append(f"### {source} — {len(dispo)} disponibles, {len(vendus)} vendus\n")
        for item in sorted(dispo, key=lambda x: -x['price_ref']):
            lines.append(f"- **{item['price_ref']}€** — {item['title']} — [voir]({item['url']})")
        if vendus:
            lines.append("")
            for item in sorted(vendus, key=lambda x: -x['price_ref']):
                lines.append(f"- ~~**{item['price_ref']}€**~~ — {item['title']} [VENDU]")
        lines.append("")

    with open(ALERT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"\nRapport généré — {len(opportunites)} opportunités | {len(nouveaux_ref)} nouveaux | batch {len(batch)}/{len(actifs)}")


if __name__ == "__main__":
    main()
