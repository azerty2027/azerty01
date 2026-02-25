"""
VINYL SCOUT v3
Phase 1 : scrape sources experts
Phase 2 : cherche les memes titres sur eBay et Leboncoin <= 40% du prix ref
"""

import requests
from bs4 import BeautifulSoup
import json, re, os
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}
MIN_PRICE = 100
MAX_PRICE_RATIO = 0.40  # alerte si prix trouve <= 40% du prix ref
DB_FILE = "vinyl_db.json"
ALERT_FILE = "ALERTES.md"

def extract_price(text):
    if not text:
        return None
    text = str(text).replace('\xa0','').replace(',','.').replace(' ','')
    match = re.search(r'(\d{2,4}\.?\d*)\s*€', text)
    return float(match.group(1)) if match else None

def scrape_victorkiswell():
    results = {}
    for page in range(1, 30):
        try:
            r = requests.get(f"http://www.victorkiswell.com/v3/?page_id=15&paged={page}", headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('li.product')
            if not items:
                break
            for item in items:
                title_el = item.select_one('.woocommerce-loop-product__title') or item.select_one('h2')
                price_el = item.select_one('.price')
                link_el = item.select_one('a.woocommerce-LoopProduct-link')
                if not title_el or not price_el or not link_el:
                    continue
                price = extract_price(price_el.get_text())
                if not price or price < MIN_PRICE:
                    continue
                url = link_el['href']
                if url in results:
                    continue
                results[url] = {
                    'source': 'Victor Kiswell',
                    'title': title_el.get_text(strip=True),
                    'price_ref': price,
                    'url': url,
                    'sold': False
                }
        except Exception as e:
            print(f"VK page {page}: {e}")
            break
    print(f"Victor Kiswell: {len(results)} disques")
    return list(results.values())

def scrape_diggersdigest():
    results = {}
    for page in range(1, 20):
        try:
            r = requests.get(f"https://www.diggersdigest.com/products?page={page}", headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('li.product')
            if not items:
                break
            for item in items:
                title_el = item.select_one('.product-title')
                price_el = item.select_one('.product-price')
                link_el = item.select_one('a')
                if not title_el or not price_el or not link_el:
                    continue
                price = extract_price(price_el.get_text())
                if not price or price < MIN_PRICE:
                    continue
                url = "https://www.diggersdigest.com" + link_el['href']
                if url in results:
                    continue
                sold = 'sold' in price_el.get_text().lower()
                results[url] = {
                    'source': "Digger's Digest",
                    'title': title_el.get_text(strip=True),
                    'price_ref': price,
                    'url': url,
                    'sold': sold
                }
        except Exception as e:
            print(f"DD page {page}: {e}")
            break
    print(f"Digger's Digest: {len(results)} disques")
    return list(results.values())

def scrape_diaspora():
    results = {}
    for page in range(0, 15):
        try:
            url = f"https://www.diasporarecords.com/search?f[0]=new_arrivals:1&page={page}"
            r = requests.get(url, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('article')
            if not items:
                break
            found_new = False
            for item in items:
                link_el = item.select_one('a')
                title_el = item.select_one('h2, h3')
                if not link_el or not title_el:
                    continue
                href = link_el.get('href', '')
                full_url = 'https://www.diasporarecords.com' + href if href.startswith('/') else href
                if full_url in results:
                    continue
                price_match = re.search(r'([\d]+[,.]?\d*)\s*€', item.get_text())
                if not price_match:
                    continue
                price = extract_price(price_match.group())
                if not price or price < MIN_PRICE:
                    continue
                results[full_url] = {
                    'source': 'Diaspora Records',
                    'title': title_el.get_text(strip=True),
                    'price_ref': price,
                    'url': full_url,
                    'sold': False
                }
                found_new = True
            if not found_new:
                break
        except Exception as e:
            print(f"Diaspora page {page}: {e}")
            break
    print(f"Diaspora Records: {len(results)} disques")
    return list(results.values())

def clean_title(title):
    """Extrait les mots cles du titre pour la recherche."""
    title = re.sub(r'[^\w\s]', ' ', title)
    words = title.split()
    # Garde les mots significatifs (plus de 2 caracteres)
    words = [w for w in words if len(w) > 2]
    return ' '.join(words[:5])  # Max 5 mots pour la recherche

def search_ebay(title, max_price):
    """Cherche sur eBay.fr et eBay.co.uk."""
    results = []
    query = clean_title(title)
    for domain in ['ebay.fr', 'ebay.co.uk']:
        try:
            url = f"https://www.{domain}/sch/i.html"
            params = {
                '_nkw': query + ' vinyl',
                '_sop': '10',  # tri par prix croissant
                'LH_BIN': '1',  # achat immediat
                '_udhi': str(int(max_price)),  # prix max
            }
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('.s-item')
            for item in items:
                title_el = item.select_one('.s-item__title')
                price_el = item.select_one('.s-item__price')
                link_el = item.select_one('a.s-item__link')
                if not title_el or not price_el or not link_el:
                    continue
                if 'Shop on eBay' in title_el.get_text():
                    continue
                price = extract_price(price_el.get_text())
                if not price or price > max_price:
                    continue
                results.append({
                    'platform': domain,
                    'title': title_el.get_text(strip=True),
                    'price': price,
                    'url': link_el['href']
                })
        except Exception as e:
            print(f"eBay {domain}: {e}")
    return results

def search_leboncoin(title, max_price):
    """Cherche sur Leboncoin."""
    results = []
    query = clean_title(title)
    try:
        url = "https://www.leboncoin.fr/recherche"
        params = {
            'text': query,
            'category': '34',  # CD et vinyles
            'price': f'0-{int(max_price)}',
        }
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        items = soup.select('a[data-qa-id="aditem_container"]')
        for item in items:
            title_el = item.select_one('p[data-qa-id="aditem_title"]')
            price_el = item.select_one('span[data-qa-id="aditem_price"]')
            if not title_el or not price_el:
                continue
            price = extract_price(price_el.get_text())
            if not price or price > max_price:
                continue
            results.append({
                'platform': 'leboncoin.fr',
                'title': title_el.get_text(strip=True),
                'price': price,
                'url': 'https://www.leboncoin.fr' + item.get('href', '')
            })
    except Exception as e:
        print(f"Leboncoin: {e}")
    return results

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE) as f:
            return json.load(f)
    return {}

def save_db(db):
    with open(DB_FILE, 'w') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def main():
    print("Phase 1 : scraping sources experts...")
    all_records = []
    all_records += scrape_victorkiswell()
    all_records += scrape_diggersdigest()
    all_records += scrape_diaspora()
    print(f"TOTAL: {len(all_records)} disques uniques >= {MIN_PRICE}EUR")

    db = load_db()
    nouveaux_ref = []
    for r in all_records:
        key = r['url']
        if key not in db:
            nouveaux_ref.append(r)
            db[key] = {**r, 'first_seen': datetime.now().isoformat()}
    save_db(db)

    print(f"\nPhase 2 : recherche opportunites sur eBay et Leboncoin...")
    opportunites = []
    for record in all_records:
        if record.get('sold'):
            continue
        max_price = round(record['price_ref'] * MAX_PRICE_RATIO, 0)
        found = []
        found += search_ebay(record['title'], max_price)
        found += search_leboncoin(record['title'], max_price)
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
                'max_achat': max_price
            })

    opportunites.sort(key=lambda x: -x['marge'])

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"# VINYL SCOUT — Rapport du {now}\n"]
    lines.append(f"**Base de reference : {len(db)} disques >= {MIN_PRICE}€**\n")
    lines.append(f"**Seuil achat : <= {int(MAX_PRICE_RATIO*100)}% du prix de reference**\n")
    lines.append("---\n")

    if opportunites:
        lines.append(f"## 🔴 {len(opportunites)} OPPORTUNITES DETECTEES\n")
        for o in opportunites:
            lines.append(f"### {o['ref_title']}")
            lines.append(f"- **Prix ref** : {o['ref_price']}€ ({o['ref_source']})")
            lines.append(f"- **Trouve** : {o['found_price']}€ sur {o['platform']}")
            lines.append(f"- **Marge potentielle** : {o['marge']}€ ({o['marge_pct']}%)")
            lines.append(f"- **Lien** : {o['found_url']}")
            lines.append("")
    else:
        lines.append("## ✅ Aucune opportunite detectee aujourd'hui\n")

    if nouveaux_ref:
        lines.append(f"---\n## 🆕 {len(nouveaux_ref)} nouveaux disques en reference\n")
        for r in nouveaux_ref:
            lines.append(f"- **{r['price_ref']}€** — {r['title']} ({r['source']})")
        lines.append("")

    lines.append("---\n## Base complete\n")
    by_source = {}
    for r in all_records:
        by_source.setdefault(r['source'], []).append(r)
    for source, items in sorted(by_source.items()):
        lines.append(f"### {source} — {len(items)} disques\n")
        for item in sorted(items, key=lambda x: -x['price_ref']):
            sold = "~~VENDU~~ " if item.get('sold') else ""
            l
