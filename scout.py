"""
VINYL SCOUT v5
- Scraping profond Kiswell + Digger's Digest + Superfly + Diaspora
- Phase 2 : opportunites Leboncoin
"""

import requests
from bs4 import BeautifulSoup
import json, re, os, time
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
MIN_PRICE = 100
MAX_PRICE_RATIO = 0.40
DB_FILE = "vinyl_db.json"
ALERT_FILE = "ALERTES.md"

def extract_price(text):
    if not text:
        return None
    text = str(text).replace('\xa0','').replace('\u202f','').replace(',','.').replace(' ','')
    match = re.search(r'(\d{2,4}\.?\d*)\s*[€£]', text)
    if match:
        return float(match.group(1))
    match = re.search(r'(\d{2,4}\.?\d*)', text)
    return float(match.group(1)) if match else None

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
                    price_text = price_el.get_text() if price_el else ''
                    price = extract_price(price_text)
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
        ('soul-funk-disco', '99000179'),
        ('jazz', '99000181'),
        ('afro', '99000182'),
        ('latin', '99000187'),
        ('brasil', '99000183'),
        ('european', '99000185'),
        ('reggae', '99000184'),
        ('new-grooves-hip-hop', '99000191'),
        ('groovy-rock', '99000190'),
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
                # Chaque item : image link + artist link + title link + prix
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
                    # Titre = texte du lien (artist - title format)
                    title = link.get_text(strip=True)
                    if not title:
                        # Chercher dans le parent
                        parent = link.find_parent()
                        if parent:
                            texts = [a.get_text(strip=True) for a in parent.select('a[href*="/item/"]')]
                            title = ' - '.join(t for t in texts if t)
                    if not title:
                        continue
                    # Prix : chercher le texte contenant € proche du lien
                    parent_block = link.find_parent(['div', 'li', 'td'])
                    price_text = parent_block.get_text() if parent_block else ''
                    price = extract_price(price_text)
                    if not price or price < MIN_PRICE:
                        continue
                    sold = 'sold' in price_text.lower() or 'vendu' in price_text.lower()
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
            # Items : liens avec titre + prix en texte
            items = soup.select('a[href^="/"]')
            items = [a for a in items if a.get_text(strip=True) and '€' not in a.get_text()]
            # Chercher blocs contenant prix
            price_blocks = []
            for tag in soup.find_all(string=re.compile(r'\d+,\d+\s*€')):
                parent = tag.find_parent()
                if parent:
                    price_blocks.append(parent)
            # Approche directe : parcourir les blocs article/div visibles
            blocks = soup.select('article') or soup.select('.views-row') or soup.select('[class*="record"]')
            if not blocks:
                # Fallback : chercher tous les liens qui pointent vers des disques
                blocks = [a.find_parent(['div', 'li']) for a in soup.select('a[href*="/jazz"], a[href*="/afro"], a[href*="/soul"], a[href*="/africa"], a[href*="/caribbean"], a[href*="/latin"], a[href*="/brasil"]') if a.find_parent(['div', 'li'])]
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
                if 'diasporarecords.com' not in full_url:
                    continue
                if full_url in results:
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

def clean_title(title):
    title = re.sub(r'[^\w\s]', ' ', title)
    words = [w for w in title.split() if len(w) > 2]
    return ' '.join(words[:5])

def search_leboncoin(title, max_price):
    results = []
    query = clean_title(title)
    try:
        params = {
            'text': query,
            'category': '34',
            'price': f'0-{int(max_price)}',
        }
        r = requests.get("https://www.leboncoin.fr/recherche", headers=HEADERS, params=params, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        for item in soup.select('a[data-qa-id="aditem_container"]'):
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
        time.sleep(1)
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
    all_records += scrape_superfly()
    all_records += scrape_diaspora()

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

    print(f"\nPhase 2 : recherche opportunites Leboncoin...")
    opportunites = []
    actifs = [r for r in all_records if not r.get('sold')]
    for record in actifs:
        max_price = round(record['price_ref'] * MAX_PRICE_RATIO, 0)
        found = search_leboncoin(record['title'], max_price)
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
    opportunites.sort(key=lambda x: -x['marge'])

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    lines = [f"# VINYL SCOUT — Rapport du {now}\n"]
    lines.append(f"**Base : {len(db)} disques | Actifs : {len(actifs)} | Seuil achat : {int(MAX_PRICE_RATIO*100)}% du prix ref**\n")
    lines.append("---\n")

    if opportunites:
        lines.append(f"## 🔴 {len(opportunites)} OPPORTUNITES LEBONCOIN\n")
        for o in opportunites:
            lines.append(f"### {o['ref_title']}")
            lines.append(f"- **Ref** : {o['ref_price']}€ chez {o['ref_source']}")
            lines.append(f"- **Trouve** : {o['found_price']}€ sur {o['platform']} — marge potentielle **{o['marge']}€ ({o['marge_pct']}%)**")
            lines.append(f"- [Voir l'annonce]({o['found_url']})")
            lines.append("")
    else:
        lines.append("## ✅ Aucune opportunite aujourd'hui\n")

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

    print(f"Rapport genere — {len(opportunites)} opportunites | {len(nouveaux_ref)} nouveaux")

if __name__ == "__main__":
    main()
