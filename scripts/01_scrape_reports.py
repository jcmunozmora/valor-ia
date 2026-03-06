"""
SROI Reports Scraper - Social Value UK
Descarga metadatos de los 468 reportes del sitemap de socialvalueuk.org
Guarda en JSON y CSV para posterior procesamiento.
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import os
import re
from datetime import datetime
from pathlib import Path

# Configuracion
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports" / "pdfs"
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

SITEMAP_URL = "https://socialvalueuk.org/reports-sitemap.xml"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}
DELAY = 1.5  # segundos entre requests


def get_report_urls():
    """Extrae solo las URLs de reportes del sitemap XML (excluye imagenes y otros)."""
    print(f"Fetching sitemap: {SITEMAP_URL}")
    resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml-xml")
    all_urls = [loc.text.strip() for loc in soup.find_all("loc")]
    # Filtrar solo URLs de reportes (excluir imagenes, uploads, etc.)
    report_urls = [
        u for u in all_urls
        if "/reports/" in u and "wp-content" not in u and not u.endswith((".png", ".jpg", ".jpeg", ".gif", ".pdf"))
    ]
    print(f"Found {len(all_urls)} total URLs, {len(report_urls)} report pages in sitemap")
    return report_urls


def parse_report_page(url):
    """Extrae metadatos de una pagina de reporte individual."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 404:
            return {"url": url, "error": "404 Not Found"}
        resp.raise_for_status()
    except Exception as e:
        return {"url": url, "error": str(e)}

    soup = BeautifulSoup(resp.content, "html.parser")

    report = {
        "url": url,
        "slug": url.rstrip("/").split("/")[-1],
        "scraped_at": datetime.now().isoformat(),
        "error": None,
    }

    # Titulo
    h1 = soup.find("h1")
    report["title"] = h1.get_text(strip=True) if h1 else None

    # Buscar campos del post/entry
    # Usar body completo para extraccion
    body = soup.body or soup

    # Extraer el texto completo de la pagina (excluir nav/footer)
    for tag in body.find_all(["nav", "footer", "header", "script", "style"]):
        tag.decompose()

    full_text = body.get_text(separator="\n", strip=True)
    report["full_text"] = full_text[:8000]

    # Buscar PDF links en TODO el body
    pdf_links = []
    for a in body.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower() or ("wp-content/uploads" in href and href):
            if href not in pdf_links:
                pdf_links.append(href)
    report["pdf_urls"] = pdf_links
    report["pdf_url"] = pdf_links[0] if pdf_links else None

    # Extraer Abstract: buscar el bloque despues del heading "Abstract"
    abstract_text = None
    # Metodo 1: buscar h2/h3 con texto "Abstract"
    for heading in body.find_all(["h2", "h3", "h4", "strong", "b"]):
        if "abstract" in heading.get_text(strip=True).lower():
            # Recoger el texto que sigue
            next_content = []
            for sibling in heading.find_next_siblings(["p", "div"]):
                txt = sibling.get_text(strip=True)
                if txt and len(txt) > 20:
                    next_content.append(txt)
                    if len(" ".join(next_content)) > 1500:
                        break
            if next_content:
                abstract_text = " ".join(next_content)[:2000]
            break
    # Metodo 2: regex en el texto completo (single newline separator)
    if not abstract_text:
        match = re.search(r"Abstract\s*\n([\s\S]{50,3000}?)(?:\n(?:Download|View|Report|References|Related|\Z))", full_text, re.IGNORECASE)
        if match:
            abstract_text = match.group(1).strip()
        else:
            # Fallback: todo el texto despues de "Abstract"
            idx = full_text.find("Abstract\n")
            if idx >= 0:
                abstract_text = full_text[idx+9:idx+3000].strip()

    # Extraer ratio SROI del texto
    sroi_match = re.search(
        r"SROI\s+(?:ratio\s+)?(?:of\s+)?(\d+\.?\d*)|"
        r"(?:ratio|return)\s+(?:of\s+)?(\d+\.?\d*)\s*[:;]|"
        r"[£\$](\d+\.?\d*)\s+(?:for every|per)\s+[£\$]1",
        full_text, re.IGNORECASE
    )
    sroi_ratio = None
    if sroi_match:
        sroi_ratio = next(g for g in sroi_match.groups() if g)

    # Campos estructurados con etiquetas comunes
    def find_field(labels):
        for label in labels:
            pattern = re.compile(label, re.IGNORECASE)
            # Buscar en texto con patron "Label: Value"
            match = re.search(rf"(?:{label})[:\s]+([^\n]{{3,100}})", full_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None

    report["sroi_ratio"] = sroi_ratio or find_field([r"SROI", r"social return"])
    report["organization"] = find_field(["organisation", "organization", "client", "company"])
    report["country"] = find_field(["country", "location", "region"])
    report["year"] = find_field(["year", "date published", "published"])
    report["report_type"] = find_field(["report type", "type of report", "forecast", "evaluative"])
    report["sector"] = find_field(["sector", "topic", "theme", "area"])
    report["assurance"] = find_field(["assured", "assurance"])

    # Fecha de publicacion: buscar en meta tags
    date_meta = soup.find("meta", {"property": "article:published_time"}) or \
                soup.find("meta", {"name": "date"}) or \
                soup.find("time")
    if date_meta:
        report["published_date"] = date_meta.get("content") or date_meta.get("datetime") or date_meta.get_text(strip=True)
    else:
        # Intentar extraer del URL de uploads de PDF (ej: /2026/03/)
        if pdf_links:
            date_match = re.search(r"/uploads/(\d{4})/(\d{2})/", pdf_links[0])
            if date_match:
                report["published_date"] = f"{date_match.group(1)}-{date_match.group(2)}"
        if not report.get("published_date"):
            report["published_date"] = None

    # Abstract final
    report["abstract"] = abstract_text

    # OG image
    og_image = soup.find("meta", {"property": "og:image"})
    report["thumbnail_url"] = og_image["content"] if og_image and og_image.get("content") else None

    # Categoria/tags de WordPress
    categories = []
    for tag in soup.find_all(class_=re.compile(r"category|tag|topic")):
        txt = tag.get_text(strip=True)
        if txt and len(txt) < 50:
            categories.append(txt)
    report["categories"] = list(set(categories))[:10]

    return report


def save_checkpoint(reports, checkpoint_path):
    """Guarda progreso parcial."""
    with open(checkpoint_path, "w", encoding="utf-8") as f:
        json.dump(reports, f, ensure_ascii=False, indent=2)


def main():
    checkpoint_path = DATA_DIR / "reports_checkpoint.json"
    output_json = DATA_DIR / "reports_metadata.json"
    output_csv = DATA_DIR / "reports_metadata.csv"

    # Cargar checkpoint si existe
    scraped_reports = []
    scraped_urls = set()
    if checkpoint_path.exists():
        with open(checkpoint_path, "r", encoding="utf-8") as f:
            scraped_reports = json.load(f)
        scraped_urls = {r["url"] for r in scraped_reports}
        print(f"Resuming from checkpoint: {len(scraped_reports)} reports already scraped")

    # Obtener URLs del sitemap
    all_urls = get_report_urls()
    pending = [u for u in all_urls if u not in scraped_urls]
    print(f"Pending: {len(pending)} reports to scrape")

    # Scraping
    for i, url in enumerate(pending):
        print(f"[{i+1}/{len(pending)}] {url}")
        report = parse_report_page(url)
        scraped_reports.append(report)

        # Checkpoint cada 25 reportes
        if (i + 1) % 25 == 0:
            save_checkpoint(scraped_reports, checkpoint_path)
            print(f"  -> Checkpoint saved ({len(scraped_reports)} total)")

        time.sleep(DELAY)

    # Guardar resultado final JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(scraped_reports, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(scraped_reports)} reports to {output_json}")

    # Guardar CSV (campos planos)
    csv_fields = [
        "url", "slug", "title", "published_date", "abstract", "organization",
        "country", "year", "report_type", "sector", "assurance",
        "sroi_ratio", "pdf_url", "thumbnail_url", "scraped_at", "error"
    ]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(scraped_reports)
    print(f"Saved CSV to {output_csv}")

    # Stats
    with_pdf = sum(1 for r in scraped_reports if r.get("pdf_url"))
    with_errors = sum(1 for r in scraped_reports if r.get("error"))
    print(f"\nStats: {len(scraped_reports)} total | {with_pdf} with PDF | {with_errors} errors")


if __name__ == "__main__":
    main()
