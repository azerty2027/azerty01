"""
VINYL SCOUT v2 - Version corrigee
- Deduplication par URL
- Rapport propre sans doublons
"""

import requests
from bs4 import BeautifulSoup
import json, re, os
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}
MIN_PRICE = 100
DB_FILE = "vinyl_db.json"
ALERT_FILE = "ALERTES.md"

def extract_price(text):
    if not text:
        return None
    text = str(text).replace('\xa0','').replace(',','.').replace(' ','')
    match = re.search(r'(\d{2,4}\.?\d*)\s*€', text)
    return float(match.group(1)) if match else None

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

def scrape_superfly():
    results = {}
    cats = [
        "https://www.superflyrecords.com/listing/2/0-99000179-0/0_{}/superflyrecords-soul-funk-disco.html",
        "https://www.superflyrecords.com/listing/2/0-99000181-0/0_{}/superflyrecords-jazz.html",
        "https://www.superflyrecords.com/listing/2/0-99000182-0/0_{}/superflyrecords-afro.html",
        "https://www.superflyrecords.com/listing/2/0-99000187-0/0_{}/superflyrecords-latin.html",
        "https://www.superflyrecords.com/listing/2/0-99000183-0/0_{}/superflyrecords-brasil.html",
    ]
    for cat in cats:
        for page in range(1, 20):
            try:
                r = requests.get(cat.format(page), headers=HEADERS, timeout=20)
                soup = BeautifulSoup(r.text, 'html.parser')
                items = soup.select('.item')
                if not items:
                    break
                found_new = False
                for item in items:
                    link_el = item.select_one('a')
                    if not link_el:
                        continue
                    href = link_el.get('href', '')
                    if not href or href in results:
                        continue
                    text = item.get_text(separator=' ')
                    price = extract_price(text)
                    if not price or price < MIN_PRICE:
                        continue
                    title_el = item.select_one('.item-artist, .item-title, strong')
                    title = title_el.get_text(strip=True) if title_el else text[:80]
                    url = href if href.startswith('http') else 'https://www.superflyrecords.com' + href
                    results[url] = {
                        'source': 'Superfly Records',
                        'title': title,
                        'price_ref': price,
                        'url': url,
                        'sold': False
                    }
                    found_new = True
                if not found_new:
                    break
            except Exception as e:
                print(f"SF: {e}")
                break
    print(f"Superfly Records: {len(results)} disques")
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

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE) as f:
            return json.load(f)
    return {}

def save_db(db):
    with open(DB_FILE, 'w') as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def main():
    print("Scraping en cours...")
    all_records = []
    all_records += scrape_diggersdigest()
    all_records += scrape_victorkiswell()
    all_records += scrape_superfly()
    all_records += scrape_diaspora()
    print(f"TOTAL: {len(all_records)} disques uniques >= {MIN_PRICE}EUR")

    db = load_db()
    nouveaux = []
    for r in all_records:
        key = r['url']
        if key not in db:
            nouveaux.append(r)
            db[key] = {**r, 'first_seen': datetime.now().isoformat()}
    save_db(db)

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"# VINYL SCOUT — Rapport du {now}\n"]
    lines.append(f"**Base de reference : {len(db)} disques uniques >= {MIN_PRICE}€**\n")
    lines.append("---\n")

    if nouveaux:
        lines.append(f"## 🔴 {len(nouveaux)} NOUVEAUX DISQUES\n")
        for r in nouveaux:
            lines.append(f"### {r['title']}")
            lines.append(f"- **Source** : {r['source']}")
            lines.append(f"- **Prix** : {r['price_ref']}€")
            lines.append(f"- **Lien** : {r['url']}")
            lines.append("")
    else:
        lines.append("## ✅ Aucun nouveau disque\n")

    lines.append("---\n")
    lines.append("## Base complete\n")
    by_source = {}
    for r in all_records:
        by_source.setdefault(r['source'], []).append(r)
    for source, items in sorted(by_source.items()):
        lines.append(f"### {source} — {len(items)} disques\n")
        for item in sorted(items, key=lambda x: -x['price_ref']):
            sold = "~~VENDU~~ " if item.get('sold') else ""
            lines.append(f"- {sold}**{item['price_ref']}€** — {item['title']} — [voir]({item['url']})")
        lines.append("")

    with open(ALERT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"Rapport: {ALERT_FILE} — {len(nouveaux)} nouveaux")

if __name__ == "__main__":
    main()
