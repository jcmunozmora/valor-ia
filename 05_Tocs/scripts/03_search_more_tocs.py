"""
03_search_more_tocs.py
-----------------------
Busca más ToCs automáticamente usando búsquedas Google (via requests + scraping)
y los agrega a toc_database.csv.

Estrategia:
  1. Búsquedas predefinidas por sector y tipo de organización
  2. Filtra resultados que sean PDFs o páginas con "theory of change" en la URL/título
  3. Agrega nuevas entradas al CSV con estado "pendiente"

Dependencias: requests, beautifulsoup4
Instalar: pip install requests beautifulsoup4
"""

import csv
import time
import hashlib
import re
from pathlib import Path
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
CSV_PATH = BASE_DIR / "metadata" / "toc_database.csv"

# ── Consultas de búsqueda por sector ──────────────────────────────────────────
# Formato: (consulta, sector, subsector)
SEARCH_QUERIES = [
    # Metodología
    ('theory of change guide PDF nonprofit filetype:pdf', "Metodología", "Guía general"),
    ('teoria del cambio guia PDF ONG filetype:pdf', "Metodología", "Guía en español"),
    # Salud
    ('theory of change health program PDF site:who.int OR site:paho.org', "Salud", "Salud pública OMS/OPS"),
    ('theory of change maternal health child health PDF NGO', "Salud", "Salud materna e infantil"),
    ('theory of change HIV AIDS prevention PDF filetype:pdf', "Salud", "VIH/SIDA"),
    # Educación
    ('theory of change education program PDF primary secondary school', "Educación", "Educación básica"),
    ('theory of change early childhood education PDF program', "Educación", "Primera infancia"),
    ('theory of change higher education university access PDF', "Educación", "Educación superior"),
    # Empleo / Juventud
    ('theory of change youth employment vocational training PDF Africa Asia', "Empleo / Juventud", "Formación vocacional"),
    ('theory of change workforce development social enterprise PDF', "Empleo / Juventud", "Empresa social"),
    # Nutrición / Alimentación
    ('theory of change stunting wasting malnutrition program PDF', "Nutrición", "Desnutrición infantil"),
    ('theory of change smallholder farmer agriculture livelihoods PDF Africa', "Nutrición", "Agricultura familiar"),
    # Vivienda
    ('theory of change affordable housing homelessness PDF program', "Vivienda", "Vivienda asequible"),
    ('teoria del cambio vivienda social programa PDF Colombia', "Vivienda", "Vivienda Colombia"),
    # Medio ambiente / Clima
    ('theory of change climate change adaptation mitigation program PDF', "Clima", "Adaptación/Mitigación"),
    ('theory of change biodiversity conservation PDF NGO', "Clima", "Biodiversidad"),
    # Género / Inclusión
    ('theory of change gender equality women empowerment PDF program', "Género", "Empoderamiento mujer"),
    ('theory of change disability inclusion social program PDF', "Inclusión", "Discapacidad"),
    # Gobernanza / Justicia
    ('theory of change governance anti-corruption civil society PDF', "Gobernanza", "Anticorrupción"),
    ('theory of change criminal justice reintegration reentry PDF', "Justicia", "Reinserción"),
    # SROI / Valoración
    ('SROI report theory of change impact map PDF filetype:pdf', "SROI", "Impacto social SROI"),
    ('social value theory of change program evaluation PDF', "SROI", "Valor social"),
    # América Latina
    ('teoria del cambio programa social Colombia filetype:pdf', "América Latina", "Colombia"),
    ('teoria del cambio programa social Mexico Argentina Brazil PDF', "América Latina", "México/Argentina/Brasil"),
    ('theory of change IDB IADB Latin America social program PDF', "América Latina", "BID/IADB"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


def load_existing_urls() -> set:
    """Carga URLs ya en el CSV para evitar duplicados."""
    if not CSV_PATH.exists():
        return set()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return {row.get("url", "") for row in csv.DictReader(f)}


def get_next_id(csv_path: Path) -> int:
    """Retorna el siguiente ID disponible en el CSV."""
    if not csv_path.exists():
        return 1
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return 1
    return max(int(r.get("id", 0)) for r in rows) + 1


def google_search(query: str, num_results: int = 10) -> list[dict]:
    """
    Busca en Google y retorna lista de {title, url, snippet}.
    Nota: Para producción usar SerpAPI o Google Custom Search API.
    Esta implementación usa scraping básico (puede ser bloqueada).
    """
    url = f"https://www.google.com/search?q={quote_plus(query)}&num={num_results}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for g in soup.find_all("div", class_="g")[:num_results]:
            a = g.find("a")
            title_el = g.find("h3")
            snippet_el = g.find("div", {"data-sncf": "1"}) or g.find("span", class_="aCOpRe")
            if a and title_el:
                results.append({
                    "title":   title_el.get_text(),
                    "url":     a.get("href", ""),
                    "snippet": snippet_el.get_text() if snippet_el else "",
                })
        return results
    except Exception as e:
        print(f"  Error buscando '{query[:50]}': {e}")
        return []


def is_relevant(result: dict) -> bool:
    """Filtra resultados relevantes: PDFs o páginas con ToC en URL/título."""
    url   = result.get("url", "").lower()
    title = result.get("title", "").lower()
    text  = (url + " " + title + " " + result.get("snippet", "")).lower()

    # Debe mencionar theory of change o teoría del cambio
    has_toc = bool(re.search(
        r"theory.of.change|teoría.del.cambio|teoria.del.cambio|impact.map|mapa.de.impacto",
        text
    ))

    # URLs a excluir (wikis, redes sociales, tiendas)
    excluded = ["wikipedia", "twitter", "linkedin", "facebook", "amazon",
                "youtube", "shop", "store", "#", "javascript:"]
    is_excluded = any(ex in url for ex in excluded)

    return has_toc and not is_excluded


def infer_org(url: str, title: str) -> tuple[str, str]:
    """Infiere organización y tipo desde la URL."""
    domain = urlparse(url).netloc.replace("www.", "")
    org_map = {
        "unicef.org": ("UNICEF", "ONU"),
        "undp.org":   ("UNDP", "ONU"),
        "who.int":    ("WHO/OMS", "ONU"),
        "worldbank.org": ("World Bank", "Organismo multilateral"),
        "oxfam.org":  ("Oxfam", "ONG Internacional"),
        "savethechildren": ("Save the Children", "ONG Internacional"),
        "gatesfoundation.org": ("Gates Foundation", "Fundación"),
        "bridgespan.org": ("Bridgespan", "Consultoría"),
        "urban.org":  ("Urban Institute", "Think tank"),
        "nesta.org":  ("Nesta", "Think tank"),
        ".gov":       ("Gobierno", "Gobierno"),
        ".edu":       ("Universidad", "Universidad"),
        ".ac.uk":     ("Universidad UK", "Universidad"),
        ".org":       ("Organización", "ONG"),
    }
    for key, (org, tipo) in org_map.items():
        if key in domain:
            return org, tipo
    return domain, "Desconocido"


def append_to_csv(records: list[dict]):
    """Agrega nuevos registros al CSV existente."""
    if not records:
        return

    fieldnames = [
        "id", "titulo", "organizacion", "tipo_org", "sector", "subsector",
        "pais_contexto", "año", "idioma", "tipo_documento", "nivel_toc",
        "url", "url_pdf", "descripcion", "palabras_clave", "estado_descarga"
    ]

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for rec in records:
            writer.writerow(rec)

    print(f"  → {len(records)} nuevos registros agregados al CSV")


def run():
    existing_urls = load_existing_urls()
    next_id       = get_next_id(CSV_PATH)
    new_records   = []

    print(f"\n{'='*60}")
    print(f"  BÚSQUEDA AUTOMATIZADA DE ToCs")
    print(f"  Consultas predefinidas: {len(SEARCH_QUERIES)}")
    print(f"  URLs existentes en DB: {len(existing_urls)}")
    print(f"{'='*60}\n")

    for i, (query, sector, subsector) in enumerate(SEARCH_QUERIES, 1):
        print(f"[{i:02d}/{len(SEARCH_QUERIES)}] {query[:60]}")

        results = google_search(query, num_results=10)
        relevant = [r for r in results if is_relevant(r)]
        new      = [r for r in relevant if r["url"] not in existing_urls]

        print(f"           Resultados: {len(results)} | Relevantes: {len(relevant)} | Nuevos: {len(new)}")

        for r in new:
            url   = r["url"]
            title = r["title"][:200]
            org, tipo = infer_org(url, title)
            is_pdf = url.lower().endswith(".pdf")

            record = {
                "id":           next_id,
                "titulo":       title,
                "organizacion": org,
                "tipo_org":     tipo,
                "sector":       sector,
                "subsector":    subsector,
                "pais_contexto": "Global",
                "año":          "",
                "idioma":       "EN" if re.search(r"[a-z]", title[:20]) else "ES",
                "tipo_documento": "PDF" if is_pdf else "Web",
                "nivel_toc":    "Por determinar",
                "url":          url,
                "url_pdf":      url if is_pdf else "",
                "descripcion":  r.get("snippet", "")[:300],
                "palabras_clave": f"theory of change, {sector.lower()}",
                "estado_descarga": "pendiente",
            }
            new_records.append(record)
            existing_urls.add(url)
            next_id += 1
            print(f"           + {title[:60]}")

        print()
        time.sleep(3)  # Pausa entre búsquedas

    append_to_csv(new_records)

    print(f"\n{'='*60}")
    print(f"  RESUMEN")
    print(f"  Nuevos registros encontrados: {len(new_records)}")
    print(f"  CSV actualizado: {CSV_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    print("NOTA: Este script usa scraping básico de Google.")
    print("      Para uso intensivo, considerar Google Custom Search API")
    print("      o SerpAPI con clave de API.\n")
    run()
