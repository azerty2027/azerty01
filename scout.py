"""
VINYL SCOUT
Scrape 4 sources experts, detecte les disques >= 100 EUR
et genere un rapport ALERTES.md sur GitHub
"""

import requests
from bs4 import BeautifulSoup
import json, re, os
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}
MIN_PRICE = 100
PAUSE = 2
DB_FILE = "vinyl_db.json"
ALERT_FILE = "ALERTES.md"

def extract_price(text):
    if not text:
        return None
    text = str(text).replace('\xa0','').replace(',','.').replace(' ','')
    match = re.search(r'(\d+\.?\d*)\s*€', text)
    if not match:
        match = re.search(r'(\d+\.?\d*)', text)
    return float(match.group(1)) if match else None

def scrape_diggersdigest():
    results = []
    for page in range(1, 15):
        try:
            r = requests.get(f"https://www.diggersdigest.com/products?page={page}", headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('li.product')
            if not items:
                break
            for item in items:
                title = item.select_one('.product-title')
                price_el = item.select_one('.product-price')
                link = item.select_one('a')
                if not title or not price_el:
                    continue
                price = extract_price(price_el.get_text())
                sold = 'sold' in price_el.get_text().lower()
                if price and price >= MIN_PRICE:
                    results.append({
                        'source': "Digger's Digest",
                        'title': title.get_text(strip=True),
                        'price_ref': price,
                        'url': "https://www.diggersdigest.com" + link['href'] if link else '',
                        'sold': sold
                    })
        except:
            break
    return results

def scrape_victorkiswell():
    results = []
    for page in range(1, 20):
        try:
            r = requests.get(f"http://www.victorkiswell.com/v3/?page_id=15&paged={page}", headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('.product')
            if not items:
                break
            for item in items:
                title = item.select_one('.woocommerce-loop-product__title') or item.select_one('h2')
                price_el = item.select_one('.price')
                link = item.select_one('a')
                if not title or not price_el:
                    continue
                price = extract_price(price_el.get_text())
                if price and price >= MIN_PRICE:
                    results.append({
                        'source': 'Victor Kiswell',
                        'title': title.get_text(strip=True),
                        'price_ref': price,
                        'url': link['href'] if link else '',
                        'sold': False
                    })
        except:
            break
    return results

def scrape_superfly():
    results = []
    cats = [
        "https://www.superflyrecords.com/listing/2/0-99000179-0/0_{}/superflyrecords-soul-funk-disco.html",
        "https://www.superflyrecords.com/listing/2/0-99000181-0/0_{}/superflyrecords-jazz.html",
        "https://www.superflyrecords.com/listing/2/0-99000182-0/0_{}/superflyrecords-afro.html",
        "https://www.superflyrecords.com/listing/2/0-99000187-0/0_{}/superflyrecords-latin.html",
        "https://www.superflyrecords.com/listing/2/0-99000183-0/0_{}/superflyrecords-brasil.html",
    ]
    seen = set()
    for cat in cats:
        for page in range(1, 15):
            try:
                r = requests.get(cat.format(page), headers=HEADERS, timeout=15)
                soup = BeautifulSoup(r.text, 'html.parser')
                links = soup.select('a[href*="/item/"]')
                if not links:
                    break
                found = False
                for link in links:
                    href = link.get('href','')
                    if href in seen:
                        continue
                    seen.add(href)
                    block = link.get_text(separator=' ', strip=True)
                    price = extract_price(block)
                    if price and price >= MIN_PRICE:
                        found = True
                        results.append({
                            'source': 'Superfly Records',
                            'title': block[:100],
                            'price_ref': price,
                            'url': href if href.startswith('http') else 'https://www.superflyrecords.com' + href,
                            'sold': False
                        })
                if not found:
                    break
            except:
                break
    return results

def scrape_diaspora():
    results = []
    seen = set()
    for page in range(0, 10):
        try:
            url = f"https://www.diasporarecords.com/search?f[0]=new_arrivals:1&page={page}"
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')
            items = soup.select('article')
            if not items:
                break
            found = False
            for item in items:
                link = item.select_one('a')
                title_el = item.select_one('h2, h3')
                price_text = item.get_text()
                price = extract_price(re.search(r'[\d,]+\s*€', price_text).group() if re.search(r'[\d,]+\s*€', price_text) else '')
                if not title_el or not price or price < MIN_PRICE:
                    continue
                href = link['href'] if link else ''
                if href in seen:
                    continue
                seen.add(href)
                found = True
                results.append({
                    'source': 'Diaspora Records',
                    'title': title_el.get_text(strip=True),
                    'price_ref': price,
                    'url': 'https://www.diasporarecords.com' + href if href.startswith('/') else href,
                    'sold': False
                })
            if not found:
                break
        except:
            break
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
    print("Scraping en cours...")
    all_records = []
    all_records += scrape_diggersdigest()
    all_records += scrape_victorkiswell()
    all_records += scrape_superfly()
    all_records += scrape_diaspora()
    print(f"{len(all_records)} disques >= {MIN_PRICE}EUR trouves")

    db = load_db()
    nouveaux = []
    for r in all_records:
        key = r['source'] + '|' + r['title'][:50]
        if key not in db:
            nouveaux.append(r)
            db[key] = {**r, 'first_seen': datetime.now().isoformat()}
    save_db(db)

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"# VINYL SCOUT — Rapport du {now}\n"]
    lines.append(f"**Total base de reference : {len(db)} disques**\n")
    lines.append(f"**Seuil minimum : {MIN_PRICE}€**\n")
    lines.append("---\n")

    if nouveaux:
        lines.append(f"## 🔴 {len(nouveaux)} NOUVEAUX DISQUES DETECTES\n")
        for r in nouveaux:
            lines.append(f"### {r['title']}")
            lines.append(f"- **Source** : {r['source']}")
            lines.append(f"- **Prix de reference** : {r['price_ref']}€")
            lines.append(f"- **Lien** : {r['url']}")
            lines.append("")
    else:
        lines.append("## ✅ Aucun nouveau disque depuis la derniere execution\n")

    lines.append("---\n")
    lines.append("## Base de reference complete\n")
    by_source = {}
    for r in all_records:
        by_source.setdefault(r['source'], []).append(r)
    for source, items in by_source.items():
        lines.append(f"### {source} ({len(items)} disques >= {MIN_PRICE}€)\n")
        for item in sorted(items, key=lambda x: -x['price_ref']):
            status = "~~VENDU~~" if item.get('sold') else ""
            lines.append(f"- {status} **{item['price_ref']}€** — {item['title']} — [lien]({item['url']})")
        lines.append("")

    with open(ALERT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    print(f"Rapport genere : {ALERT_FILE}")
    print(f"Nouveaux disques : {len(nouveaux)}")

if __name__ == "__main__":
    main()
