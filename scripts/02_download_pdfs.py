"""
SROI PDF Downloader
Descarga los PDFs disponibles a partir del JSON de metadatos generado por 01_scrape_reports.py
"""

import json
import os
import time
import requests
from pathlib import Path
from urllib.parse import urlparse, unquote

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
PDFS_DIR = BASE_DIR / "reports" / "pdfs"
PDFS_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}
DELAY = 2.0
MAX_SIZE_MB = 50


def sanitize_filename(name):
    """Limpia nombre de archivo."""
    name = unquote(name)
    name = os.path.basename(name)
    name = name.replace("%20", "_").replace(" ", "_")
    # Eliminar caracteres especiales
    name = "".join(c for c in name if c.isalnum() or c in "._-")
    return name[:200]


def download_pdf(url, dest_path, slug):
    """Descarga un PDF con verificacion de tamano."""
    if dest_path.exists():
        return "already_exists"

    try:
        resp = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        if resp.status_code == 404:
            return "404"
        resp.raise_for_status()

        # Verificar tipo de contenido
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            return f"not_pdf: {content_type}"

        # Verificar tamano
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > MAX_SIZE_MB * 1024 * 1024:
            return f"too_large: {int(content_length) / 1024 / 1024:.1f}MB"

        # Descargar
        with open(dest_path, "wb") as f:
            downloaded = 0
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if downloaded > MAX_SIZE_MB * 1024 * 1024:
                    dest_path.unlink()
                    return f"too_large: >{MAX_SIZE_MB}MB"

        size_kb = dest_path.stat().st_size / 1024
        return f"ok: {size_kb:.0f}KB"

    except requests.exceptions.Timeout:
        return "timeout"
    except Exception as e:
        return f"error: {e}"


def main():
    metadata_path = DATA_DIR / "reports_metadata.json"
    if not metadata_path.exists():
        print(f"ERROR: Run 01_scrape_reports.py first. File not found: {metadata_path}")
        return

    with open(metadata_path, "r", encoding="utf-8") as f:
        reports = json.load(f)

    print(f"Loaded {len(reports)} reports")
    with_pdfs = [r for r in reports if r.get("pdf_url")]
    print(f"Reports with PDF URLs: {len(with_pdfs)}")

    results = []
    for i, report in enumerate(with_pdfs):
        slug = report.get("slug", f"report_{i}")
        pdf_url = report["pdf_url"]

        # Nombre de archivo basado en slug + nombre original
        original_name = sanitize_filename(pdf_url)
        filename = f"{slug[:80]}_{original_name}" if original_name not in slug else original_name
        dest_path = PDFS_DIR / filename

        print(f"[{i+1}/{len(with_pdfs)}] {slug[:50]}...")
        status = download_pdf(pdf_url, dest_path, slug)
        print(f"  -> {status}")

        results.append({
            "slug": slug,
            "url": report.get("url"),
            "pdf_url": pdf_url,
            "local_path": str(dest_path) if "ok" in status else None,
            "status": status,
        })

        if "ok" in status:
            time.sleep(DELAY)

    # Guardar log de descargas
    log_path = DATA_DIR / "pdf_download_log.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Stats
    ok = sum(1 for r in results if r["status"].startswith("ok"))
    exists = sum(1 for r in results if "already_exists" in r["status"])
    errors = len(results) - ok - exists
    print(f"\nDownload complete: {ok} new | {exists} already existed | {errors} failed/skipped")
    print(f"Log saved to {log_path}")


if __name__ == "__main__":
    main()
