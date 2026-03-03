"""
VINYL SCOUT v17
- Phase 1 : scraping sources experts (Kiswell, DD, Superfly, Diaspora, SOFA Records)
- Phase 3 : recherche opportunites Leboncoin (ScrapeOps) + Vinted (cookie auto) + eBay (direct)

Nouveautes v17 vs v16 :
  - Blacklist automatique : après 2 apparitions, une annonce est ignorée définitivement
  - Bouton "Blacklister" dans le rapport HTML pour éliminer manuellement en 1 clic
  - Compteur blacklist visible dans le rapport
"""

import requests
from bs4 import BeautifulSoup
import json, re, os, time, urllib.parse
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}
MIN_PRICE = 100
MAX_PRICE_RATIO = 0.40
DB_FILE = "vinyl_db.json"
OFFSET_FILE = "scout_offset.json"
BLACKLIST_FILE = "scout_blacklist.json"
BLACKLIST_MAX_SEEN = 2  # Après X apparitions → blacklistée définitivement
ALERT_FILE = "ALERTES.html"
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"
SCRAPEOPS_KEY = os.environ.get("SCRAPEOPS_KEY", "")
# 50 disques × 25 crédits LBC = ~1 250 crédits/run → 20 runs/mois sur 25k crédits
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# Circuit breakers
LBC_FAILURES = 0
LBC_MAX_FAILURES = 3
VINTED_FAILURES = 0
VINTED_MAX_FAILURES = 3
EBAY_FAILURES = 0
EBAY_MAX_FAILURES = 3

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


# ─────────────────────────────────────────────
# UTILS
# ─────────────────────────────────────────────

def extract_price(text):
    if not text:
        return None
    text = str(text).replace('\xa0','').replace('\u202f','').replace(',','.').replace(' ','')
    match = re.search(r'(\d{2,4}\.?\d*)\s*[€£]', text)
    if match:
        return float(match.group(1))
    match = re.search(r'(\d{2,4}\.?\d*)', text)
    return float(match.group(1)) if match else None


def words_from(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    return set(w for w in text.split() if len(w) > 3 and not w.isdigit() and w not in STOPWORDS)


def clean_title(title):
    """
    Construit une query de recherche depuis un titre de référence.
    - Ajoute "vinyl" pour cibler les résultats vinyle
    - Pour les self-titled (artiste == album), garde le nom + vinyl
    - Évite les mots trop courts ou génériques
    """
    title_clean = re.sub(r'\(.*?\)', '', title).strip()
    parts = re.split(r'\s[–\-]\s', title_clean, maxsplit=1)
    if len(parts) == 2:
        artist = parts[0].strip()
        album = parts[1].strip()
        if artist.lower() == album.lower():
            query = artist
        else:
            artist_words = [w for w in artist.split() if len(w) > 2][:2]
            album_words = [w for w in album.split() if len(w) > 2][:2]
            query = ' '.join((artist_words + album_words)[:4])
    else:
        query = re.sub(r'[^\w\s]', ' ', title)
        words = [w for w in query.split() if len(w) > 2]
        query = ' '.join(words[:4])
    return (query + ' vinyl').strip()


def is_relevant_result(ref_title, found_title):
    """
    Filtre de pertinence : double critère pour éviter les faux positifs.
    - ratio_ref  >= 0.50 : au moins 50% des mots de la ref sont dans le trouvé
    - ratio_found >= 0.30 : au moins 30% des mots du trouvé viennent de la ref
    """
    ref_words = words_from(ref_title)
    found_words = words_from(found_title)
    if not ref_words or not found_words:
        return False
    overlap = ref_words & found_words
    if not overlap:
        return False
    ratio_ref = len(overlap) / len(ref_words)
    ratio_found = len(overlap) / len(found_words)
    return ratio_ref >= 0.50 and ratio_found >= 0.30


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
                        'source': 'Victor Kiswell', 'title': title_el.get_text(strip=True),
                        'price_ref': price, 'url': url_item, 'sold': sold
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
                    'source': "Digger's Digest", 'title': title_el.get_text(strip=True),
                    'price_ref': price, 'url': url_item, 'sold': sold
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
        ('soul-funk-disco', '99000179'), ('jazz', '99000181'), ('afro', '99000182'),
        ('latin', '99000187'), ('brasil', '99000183'), ('european', '99000185'),
        ('reggae', '99000184'), ('new-grooves-hip-hop', '99000191'), ('groovy-rock', '99000190'),
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
                image_links = soup.select('a[href*="/item/"]')
                if not image_links:
                    break
                found_new = False
                seen_urls = set()
                for link in image_links:
                    href = link.get('href', '')
                    if not href or href in seen_urls:
                        continue
                    seen_urls.add(href)
                    url_item = href if href.startswith('http') else 'https://www.superflyrecords.com' + href
                    if url_item in results:
                        continue
                    title = link.get_text(strip=True)
                    if not title:
                        parent = link.find_parent()
                        if parent:
                            texts = [a.get_text(strip=True) for a in parent.select('a[href*="/item/"]')]
                            title = ' - '.join(t for t in texts if t)
                    if not title:
                        continue
                    parent_block = link.find_parent(['div', 'li', 'td'])
                    price_text = parent_block.get_text() if parent_block else ''
                    price = extract_price(price_text)
                    if not price or price < MIN_PRICE:
                        continue
                    sold = 'sold' in price_text.lower() or 'vendu' in price_text.lower()
                    results[url_item] = {
                        'source': 'Superfly Records', 'title': title,
                        'price_ref': price, 'url': url_item, 'sold': sold
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
                    'source': 'Diaspora Records', 'title': title,
                    'price_ref': price, 'url': full_url, 'sold': sold
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
        ('afro-funk-afro-disco', 'c98000508'), ('dj-stuff-club-electronics', 'c98000509'),
        ('afro-beat', 'c98000510'), ('highlife', 'c98000511'),
        ('mali-guinea-senegal', 'c98000489'), ('congo-rumba-sukus', 'c98000512'),
        ('ethiopia-sudan-somalia', 'c98000493'), ('kenya-east-africa', 'c98000539'),
        ('maghreb-vinyl', 'c98000515'), ('lebanon-egypt', 'c98000517'),
        ('french-west-indies-zou', 'c98000519'), ('reggae-dub-ska', 'c98000521'),
        ('latin-salsa-boogaloo', 'c98000523'), ('brazilian-soul-funk-samba', 'c98000526'),
        ('bossa-nova-brazilian-jazz', 'c98000527'),
        ('japanese-jazz-funk-ambient-city-pop', 'c98000529'),
        ('soul-jazz-jazz-funk', 'c98000535'), ('free-jazz-avant-garde', 'c98000537'),
        ('french-jazz-avant-garde', 'c98000531'), ('french-grooves-songs', 'c98000532'),
    ]
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
                items = soup.select('div.product-info') or soup.select('li.product') or soup.select('[class*="product"]')
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
                        price_text = parent.get_text()
                        price = extract_price(price_text)
                        if not price or price < MIN_PRICE_SOFA:
                            continue
                        title_el = link.select_one('h3') or link.select_one('h2') or link
                        title = title_el.get_text(strip=True) if title_el else ''
                        if not title:
                            continue
                        # Séparer artiste et album si collés (ex: "Cymandecymande")
                        # Chercher h3 et h4/span séparés dans le parent
                        if parent:
                            h3 = parent.select_one('h3')
                            h4 = parent.select_one('h4') or parent.select_one('p.subtitle') or parent.select_one('span.subtitle')
                            if h3 and h4 and h3.get_text(strip=True) and h4.get_text(strip=True):
                                title = h3.get_text(strip=True) + ' - ' + h4.get_text(strip=True)
                        sold = any(x in price_text.lower() for x in ['sold', 'vendu', 'épuisé'])
                        results[url_item] = {
                            'source': 'SOFA Records', 'title': title,
                            'price_ref': price, 'url': url_item, 'sold': sold
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
                        price_text = item.get_text()
                        price = extract_price(price_text)
                        if not price or price < MIN_PRICE_SOFA:
                            continue
                        title_el = item.select_one('h3') or item.select_one('h2')
                        title = title_el.get_text(strip=True) if title_el else link_el.get_text(strip=True)
                        if not title:
                            continue
                        sold = any(x in price_text.lower() for x in ['sold', 'vendu', 'épuisé'])
                        results[url_item] = {
                            'source': 'SOFA Records', 'title': title,
                            'price_ref': price, 'url': url_item, 'sold': sold
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
# PHASE 3 — RECHERCHE OPPORTUNITES MARCHE
# ─────────────────────────────────────────────

def scrapeops_get(url):
    """Requête via ScrapeOps proxy résidentiel — utilisé UNIQUEMENT pour Leboncoin."""
    return requests.get(
        "https://proxy.scrapeops.io/v1/",
        params={
            "api_key": SCRAPEOPS_KEY,
            "url": url,
            "residential": "true",
            "country": "fr",
        },
        timeout=60
    )


# ── Leboncoin (ScrapeOps) ──────────────────────────────────────────────────────

def search_leboncoin(title, max_price):
    """Leboncoin via ScrapeOps — ~25 crédits/requête."""
    global LBC_FAILURES
    if not SCRAPEOPS_KEY:
        return []
    if LBC_FAILURES >= LBC_MAX_FAILURES:
        return []
    results = []
    query = urllib.parse.quote(clean_title(title))
    try:
        url = f"https://www.leboncoin.fr/recherche?text={query}&category=34&price=0-{int(max_price)}"
        r = scrapeops_get(url)
        print(f"  LBC status: {r.status_code} | {clean_title(title)} | max {max_price}€")
        if r.status_code != 200:
            LBC_FAILURES += 1
            print(f"  LBC erreur ({LBC_FAILURES}/{LBC_MAX_FAILURES}): {r.text[:100]}")
            return results
        LBC_FAILURES = 0
        soup = BeautifulSoup(r.text, 'html.parser')
        ads = (soup.select('a[data-qa-id="aditem_container"]') or
               soup.select('[data-test-id="ad"]') or
               soup.select('li[data-id]'))
        print(f"  LBC annonces: {len(ads)}")
        for ad in ads[:20]:
            title_el = (ad.select_one('[data-qa-id="aditem_title"]') or
                        ad.select_one('h2') or ad.select_one('p'))
            price_el = (ad.select_one('[data-qa-id="aditem_price"]') or
                        ad.select_one('[class*="price"]'))
            href = ad.get('href', '')
            if not href:
                continue
            ad_url = href if href.startswith('http') else 'https://www.leboncoin.fr' + href
            price = extract_price(price_el.get_text()) if price_el else None
            if not price or price > max_price:
                continue
            found_title = title_el.get_text(strip=True) if title_el else ''
            if not is_relevant_result(title, found_title):
                continue
            results.append({
                "platform": "leboncoin.fr",
                "title": found_title,
                "price": price, "url": ad_url
            })
        time.sleep(1)
    except Exception as e:
        LBC_FAILURES += 1
        print(f"  LBC exception ({LBC_FAILURES}/{LBC_MAX_FAILURES}): {e}")
    return results


# ── Vinted (cookie auto — 0 crédit ScrapeOps) ─────────────────────────────────

_vinted_session = None
_vinted_cookie = None

def _get_vinted_cookie():
    """Récupère un cookie de session Vinted en chargeant la homepage."""
    global _vinted_session, _vinted_cookie
    if _vinted_cookie:
        return _vinted_cookie
    try:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9",
        })
        r = session.get("https://www.vinted.fr", timeout=20)
        cookie = r.cookies.get("access_token_web") or r.cookies.get("_vinted_fr_session")
        if cookie:
            _vinted_cookie = cookie
            _vinted_session = session
            print(f"  VTD cookie OK: {cookie[:20]}...")
            return cookie
        _vinted_session = session
        print(f"  VTD session initialisée ({len(r.cookies)} cookies)")
        return None
    except Exception as e:
        print(f"  VTD cookie exception: {e}")
        return None


def search_vinted(title, max_price):
    """Vinted via API interne /api/v2/catalog/items. 0 crédit ScrapeOps."""
    global VINTED_FAILURES, _vinted_session
    if VINTED_FAILURES >= VINTED_MAX_FAILURES:
        return []
    results = []
    try:
        if _vinted_session is None:
            _get_vinted_cookie()
        if _vinted_session is None:
            VINTED_FAILURES += 1
            return []
        query = clean_title(title)
        params = {
            "search_text": query,
            "catalog[]": "139",
            "price_to": int(max_price),
            "per_page": "20",
            "order": "newest_first",
        }
        api_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "fr-FR,fr;q=0.9",
            "Referer": "https://www.vinted.fr/",
            "X-Requested-With": "XMLHttpRequest",
        }
        r = _vinted_session.get(
            "https://www.vinted.fr/api/v2/catalog/items",
            params=params, headers=api_headers, timeout=20
        )
        print(f"  VTD status: {r.status_code} | {query} | max {max_price}€")
        if r.status_code == 401:
            print("  VTD cookie expiré, réinitialisation...")
            _vinted_session = None
            _get_vinted_cookie()
            VINTED_FAILURES += 1
            return []
        if r.status_code != 200:
            VINTED_FAILURES += 1
            print(f"  VTD erreur ({VINTED_FAILURES}/{VINTED_MAX_FAILURES}): {r.text[:100]}")
            return []
        VINTED_FAILURES = 0
        data = r.json()
        items = data.get("items", [])
        print(f"  VTD articles: {len(items)}")
        for item in items:
            try:
                price = float(item.get("price", 0))
                if not price or price > max_price:
                    continue
                item_url = item.get("url") or f"https://www.vinted.fr/items/{item.get('id', '')}"
                item_title = item.get("title", "")
                if not is_relevant_result(title, item_title):
                    continue
                results.append({
                    "platform": "vinted.fr",
                    "title": item_title,
                    "price": price,
                    "url": item_url
                })
            except Exception:
                continue
        time.sleep(1)
    except Exception as e:
        VINTED_FAILURES += 1
        print(f"  VTD exception ({VINTED_FAILURES}/{VINTED_MAX_FAILURES}): {e}")
    return results


# ── eBay (scraping direct — 0 crédit ScrapeOps) ───────────────────────────────

_ebay_session = None

def _get_ebay_session():
    """Session persistante pour eBay avec headers navigateur réalistes."""
    global _ebay_session
    if _ebay_session:
        return _ebay_session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    })
    try:
        session.get("https://www.ebay.fr", timeout=15)
        time.sleep(1)
    except Exception:
        pass
    _ebay_session = session
    return session


def search_ebay(title, max_price):
    """
    eBay via scraping direct sans proxy. 0 crédit ScrapeOps.
    Log les titres retournés pour debug des faux positifs.
    """
    global EBAY_FAILURES
    if EBAY_FAILURES >= EBAY_MAX_FAILURES:
        return []
    results = []
    try:
        session = _get_ebay_session()
        query = urllib.parse.quote(clean_title(title))
        url = f"https://www.ebay.fr/sch/i.html?_nkw={query}&_sacat=306&_udhi={int(max_price)}&LH_BIN=1&_sop=10&LH_PrefLoc=3"
        r = session.get(url, timeout=20)
        print(f"  eBay status: {r.status_code} | {clean_title(title)} | max {max_price}€")
        if r.status_code != 200:
            EBAY_FAILURES += 1
            print(f"  eBay erreur ({EBAY_FAILURES}/{EBAY_MAX_FAILURES})")
            return []
        EBAY_FAILURES = 0
        soup = BeautifulSoup(r.text, 'html.parser')
        items = soup.select('li.s-item')
        if not items:
            items = (soup.select('[data-view="mi:1686|iid:1"]') or
                     soup.select('.srp-results li') or
                     soup.select('.s-item__wrapper'))
        print(f"  eBay articles: {len(items)}")
        for item in items[:20]:
            title_el = item.select_one('.s-item__title')
            if not title_el:
                continue
            title_text = title_el.get_text(strip=True)
            if 'shop on ebay' in title_text.lower() or not title_text.strip():
                continue
            price_el = (item.select_one('.s-item__price') or
                        item.select_one('[class*="price"]'))
            link_el = item.select_one('a.s-item__link') or item.select_one('a[href*="ebay.fr/itm"]')
            if not price_el or not link_el:
                continue
            price = extract_price(price_el.get_text())
            if not price or price > max_price:
                continue
            # Log debug : afficher les titres eBay trouvés avant filtre
            print(f"    eBay candidat: {title_text[:60]} | {price}€")
            if not is_relevant_result(title, title_text):
                print(f"    → rejeté (non pertinent)")
                continue
            item_url = link_el.get('href', '').split('?')[0]
            results.append({
                "platform": "ebay.fr",
                "title": title_text,
                "price": price,
                "url": item_url
            })
        time.sleep(1.5)
    except Exception as e:
        EBAY_FAILURES += 1
        print(f"  eBay exception ({EBAY_FAILURES}/{EBAY_MAX_FAILURES}): {e}")
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


def load_blacklist():
    if os.path.exists(BLACKLIST_FILE):
        with open(BLACKLIST_FILE) as f:
            return json.load(f)
    return {}

def save_blacklist(bl):
    with open(BLACKLIST_FILE, 'w') as f:
        json.dump(bl, f, ensure_ascii=False, indent=2)

def update_blacklist(blacklist, opportunites):
    """Incremente le compteur pour chaque URL. Retourne les opportunites filtrees."""
    now = datetime.now().isoformat()
    filtered = []
    newly_blacklisted = 0
    for o in opportunites:
        url = o['found_url']
        if url in blacklist and blacklist[url].get('blacklisted'):
            continue
        if url not in blacklist:
            blacklist[url] = {
                'count': 0,
                'title': o['found_title'],
                'ref_title': o['ref_title'],
                'platform': o['platform'],
                'first_seen': now,
                'last_seen': now,
                'blacklisted': False,
            }
        blacklist[url]['count'] += 1
        blacklist[url]['last_seen'] = now
        if blacklist[url]['count'] >= BLACKLIST_MAX_SEEN:
            blacklist[url]['blacklisted'] = True
            blacklist[url]['blacklisted_at'] = now
            newly_blacklisted += 1
            print(f"  Blacklistee ({blacklist[url]['count']}x) : {o['found_title'][:60]}")
        else:
            filtered.append(o)
    if newly_blacklisted:
        print(f"  {newly_blacklisted} nouvelle(s) annonce(s) blacklistee(s) ce run")
    return filtered


def load_offset():
    try:
        with open(OFFSET_FILE) as f:
            return json.load(f).get("offset", 0)
    except Exception:
        return 0


def save_offset(offset):
    with open(OFFSET_FILE, 'w') as f:
        json.dump({"offset": offset, "updated": datetime.now().isoformat()}, f)


# ─────────────────────────────────────────────
# RAPPORT HTML
# ─────────────────────────────────────────────

def generate_html_report(db, actifs, tous_actifs, opportunites, nouveaux_ref, all_records, offset, blacklist=None):
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    credits_estimes = len(actifs) * 25

    if blacklist is None:
        blacklist = {}
    nb_blacklisted = sum(1 for v in blacklist.values() if v.get('blacklisted'))

    opp_rows = ""
    for o in opportunites:
        marge_class = "high" if o['marge_pct'] >= 70 else "medium"
        url_b64 = o['found_url'].replace('"', '&quot;')
        seen_count = blacklist.get(o['found_url'], {}).get('count', 1)
        seen_badge = f'<span class="seen-count">Vu {seen_count}x</span>'
        opp_rows += f"""
        <tr class="opp-row {marge_class}" id="row-{abs(hash(o['found_url'])) % 999999}">
            <td><strong>{o['ref_title']}</strong><br><small>{o['ref_source']}</small></td>
            <td class="price">{o['ref_price']:.0f}€</td>
            <td><a href="{o['found_url']}" target="_blank">{o['found_title'][:60] or '—'}</a><br><small>{o['platform']}</small> {seen_badge}</td>
            <td class="price">{o['found_price']:.0f}€</td>
            <td class="marge"><strong>{o['marge']:.0f}€</strong><br><span class="badge">{o['marge_pct']}%</span></td>
            <td><button class="btn-blacklist" onclick="blacklistUrl('{url_b64}', this)">🚫 Blacklister</button></td>
        </tr>"""

    new_rows = ""
    for r in sorted(nouveaux_ref, key=lambda x: -x['price_ref']):
        sold_tag = '<span class="sold">VENDU</span>' if r.get('sold') else ''
        new_rows += f"<tr><td>{r['price_ref']:.0f}€ {sold_tag}</td><td>{r['title']}</td><td>{r['source']}</td></tr>"

    by_source = {}
    for r in all_records:
        by_source.setdefault(r['source'], []).append(r)

    catalogue_html = ""
    for source, items in sorted(by_source.items()):
        dispo = [i for i in items if not i.get('sold')]
        vendus = [i for i in items if i.get('sold')]
        catalogue_html += f"<h3>{source} — {len(dispo)} disponibles, {len(vendus)} vendus</h3><table><thead><tr><th>Prix</th><th>Titre</th></tr></thead><tbody>"
        for item in sorted(dispo, key=lambda x: -x['price_ref']):
            catalogue_html += f'<tr><td class="price">{item["price_ref"]:.0f}€</td><td><a href="{item["url"]}" target="_blank">{item["title"]}</a></td></tr>'
        catalogue_html += "</tbody></table>"

    opp_empty = '<div class="empty">Aucune opportunite aujourd\'hui</div>'
    new_empty = '<div class="empty">Aucun nouveau disque</div>'
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vinyl Scout — {now}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f0f0f; color: #e0e0e0; padding: 20px; }}
  h1 {{ color: #ff6b35; font-size: 1.8em; margin-bottom: 4px; }}
  h2 {{ color: #ffd700; margin: 30px 0 12px; font-size: 1.2em; border-bottom: 1px solid #333; padding-bottom: 6px; }}
  h3 {{ color: #aaa; margin: 20px 0 8px; font-size: 1em; }}
  .meta {{ color: #666; font-size: 0.85em; margin-bottom: 24px; }}
  .stats {{ display: flex; gap: 16px; margin: 16px 0; flex-wrap: wrap; }}
  .stat {{ background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 12px 18px; text-align: center; }}
  .stat-val {{ font-size: 1.8em; font-weight: bold; color: #ff6b35; }}
  .stat-lbl {{ font-size: 0.75em; color: #888; margin-top: 2px; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 16px; font-size: 0.88em; }}
  th {{ background: #1a1a1a; color: #888; padding: 8px 10px; text-align: left; font-weight: 500; }}
  td {{ padding: 8px 10px; border-bottom: 1px solid #1e1e1e; vertical-align: top; }}
  tr:hover td {{ background: #161616; }}
  a {{ color: #4db8ff; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .price {{ font-weight: bold; white-space: nowrap; color: #90ee90; }}
  .marge {{ font-weight: bold; color: #ffd700; white-space: nowrap; }}
  .badge {{ background: #333; color: #ffd700; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; }}
  .opp-row.high td {{ border-left: 3px solid #ff4444; }}
  .opp-row.medium td {{ border-left: 3px solid #ffd700; }}
  .sold {{ color: #ff4444; font-size: 0.75em; background: #2a0000; padding: 1px 5px; border-radius: 3px; }}
  .credits {{ background: #1a1a00; border: 1px solid #444400; border-radius: 6px; padding: 10px 14px; margin: 16px 0; font-size: 0.85em; color: #cccc00; }}
  small {{ color: #888; }}
  .empty {{ color: #555; font-style: italic; padding: 16px; text-align: center; }}
</style>
</head>
<body>
<h1>🎵 Vinyl Scout</h1>
<div class="meta">Rapport généré le {now}</div>

<div class="stats">
  <div class="stat"><div class="stat-val">{len(db)}</div><div class="stat-lbl">En base</div></div>
  <div class="stat"><div class="stat-val">{len(tous_actifs)}</div><div class="stat-lbl">Actifs total</div></div>
  <div class="stat"><div class="stat-val">{len(actifs)}</div><div class="stat-lbl">Batch analysé</div></div>
  <div class="stat"><div class="stat-val">{len(opportunites)}</div><div class="stat-lbl">Opportunités</div></div>
  <div class="stat"><div class="stat-val">{len(nouveaux_ref)}</div><div class="stat-lbl">Nouveaux</div></div>
</div>

<div class="credits">
  💳 Batch {offset+1}–{offset+len(actifs)} / {len(tous_actifs)} actifs | ~{credits_estimes} crédits LBC | Vinted : cookie direct (0 crédit) | eBay : scraping direct (0 crédit)
</div>

<h2>🔴 Opportunités marché ({len(opportunites)})</h2>
{"<table><thead><tr><th>Référence</th><th>Prix ref</th><th>Annonce trouvée</th><th>Prix</th><th>Marge</th></tr></thead><tbody>" + opp_rows + "</tbody></table>" if opportunites else opp_empty}

<h2>🆕 Nouveaux en base ({len(nouveaux_ref)})</h2>
{"<table><thead><tr><th>Prix</th><th>Titre</th><th>Source</th></tr></thead><tbody>" + new_rows + "</tbody></table>" if nouveaux_ref else new_empty}

<h2>📀 Catalogue complet</h2>
{catalogue_html}

</body>
</html>"""
    return html


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("VINYL SCOUT v16")
    print("Vinted: cookie direct (0 crédit) | eBay: direct (0 crédit) | LBC: ScrapeOps")
    print(f"Batch rotatif: {BATCH_SIZE} disques/run | Log eBay debug activé")
    print("=" * 60)

    # Phase 1
    print("\n⟶ Phase 1 : scraping sources experts...")
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
    print(f"TOTAL: {len(all_records)} disques uniques >= {MIN_PRICE}€")

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

    # Phase 3
    print("\n⟶ Phase 3 : recherche opportunités marché...")
    print("  Initialisation session Vinted...")
    _get_vinted_cookie()
    print("  Initialisation session eBay...")
    _get_ebay_session()

    tous_actifs = [r for r in all_records if not r.get('sold')]
    tous_actifs_sorted = sorted(tous_actifs, key=lambda x: -x['price_ref'])

    if TEST_MODE:
        print(f"  [TEST_MODE] Limité à 5 disques")
        actifs = tous_actifs_sorted[:5]
        offset = 0
    else:
        # Batch rotatif : avance de BATCH_SIZE à chaque run
        offset = load_offset()
        if offset >= len(tous_actifs_sorted):
            offset = 0
            print(f"  Offset remis à 0 (cycle complet terminé)")
        actifs = tous_actifs_sorted[offset:offset + BATCH_SIZE]
        next_offset = offset + BATCH_SIZE
        if next_offset >= len(tous_actifs_sorted):
            next_offset = 0
        save_offset(next_offset)
        print(f"  Batch : disques {offset+1}–{offset+len(actifs)} / {len(tous_actifs)} | ~{len(actifs) * 25} crédits LBC")
        print(f"  Prochain run : disques {next_offset+1}–{next_offset+BATCH_SIZE}")

    opportunites = []
    for i, record in enumerate(actifs):
        max_price = round(record['price_ref'] * MAX_PRICE_RATIO)
        print(f"  [{i+1}/{len(actifs)}] {record['title'][:50]} | ref {record['price_ref']}€ | max {max_price}€")
        found = []
        found += search_leboncoin(record['title'], max_price)
        found += search_vinted(record['title'], max_price)
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
                'marge': round(marge, 2),
                'marge_pct': ratio,
            })

        if LBC_FAILURES >= LBC_MAX_FAILURES and VINTED_FAILURES >= VINTED_MAX_FAILURES and EBAY_FAILURES >= EBAY_MAX_FAILURES:
            print("  ⚠️ Tous les circuit breakers ouverts — arrêt Phase 3")
            break

    # Déduplication par URL
    seen_urls = set()
    opportunites_uniques = []
    for o in opportunites:
        if o['found_url'] not in seen_urls:
            seen_urls.add(o['found_url'])
            opportunites_uniques.append(o)
    opportunites = opportunites_uniques

    # Blacklist : filtrer les annonces vues trop souvent
    blacklist = load_blacklist()
    opportunites = update_blacklist(blacklist, opportunites)
    save_blacklist(blacklist)
    nb_blacklisted = sum(1 for v in blacklist.values() if v.get('blacklisted'))
    print(f"  Blacklist : {nb_blacklisted} annonces ignorées au total")

    opportunites = sorted(opportunites, key=lambda x: -x['marge'])

    # Rapport HTML
    html = generate_html_report(db, actifs, tous_actifs, opportunites, nouveaux_ref, all_records, offset, blacklist)
    with open(ALERT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ Rapport généré : {ALERT_FILE}")
    print(f"   {len(opportunites)} opportunités | {len(nouveaux_ref)} nouveaux | {len(db)} en base | {nb_blacklisted} blacklistées")
    print(f"   Crédits ScrapeOps utilisés : ~{len(actifs) * 25} (LBC seulement)")
    print(f"   Circuit breakers — LBC: {LBC_FAILURES}/{LBC_MAX_FAILURES} | VTD: {VINTED_FAILURES}/{VINTED_MAX_FAILURES} | eBay: {EBAY_FAILURES}/{EBAY_MAX_FAILURES}")


if __name__ == "__main__":
    main()
