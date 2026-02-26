tat Vinyl Scout v9 — 26/02/2026
Architecture : scout.py → GitHub Actions daily → ALERTES.md
Phase 1 — Dealers référence (seuil 100€, SOFA 70€) : Victor Kiswell, Digger's Digest, Superfly Records, Diaspora Records, SOFA Records → 422 disques en base
Phase 2 — Croisement : Disques Anciens (matching artiste + album sur mots significatifs)
Phase 3 — Marché occasion : Leboncoin (catégorie 34) + Vinted (catalog 2050) via ScrapeOps proxy résidentiel (SCRAPEOPS_KEY secret GitHub) + eBay API (EBAY_APP_ID secret GitHub). Seuil : 40% du prix ref.
Bug actif SOFA Records : dans scrape_sofarecords(), le fallback récupère le texte du lien (link_el.get_text(strip=True)) qui contient artiste + titre collés sans séparateur → Cymandecymande au lieu de Cymande — Cymande. À corriger en inspectant le HTML réel de sofarecords.fr pour trouver les bonnes balises séparées.
Statut Leboncoin/Vinted : le code est en place, ScrapeOps est intégré, mais le rapport montre "Aucune opportunité marché" — à vérifier si SCRAPEOPS_KEY est bien renseigné dans les secrets GitHub Actions et si les sélecteurs CSS sont toujours valides.
