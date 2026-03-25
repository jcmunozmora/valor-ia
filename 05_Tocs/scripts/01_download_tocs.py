"""
01_download_tocs.py
-------------------
Descarga los PDFs de la base de datos toc_database.csv al directorio pdfs/.
Usa checkpoints para reanudar descargas interrumpidas.

Dependencias: requests, pandas
Instalar: pip install requests pandas
"""

import csv
import json
import os
import time
import hashlib
from pathlib import Path
import requests
from urllib.parse import urlparse

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent.parent
CSV_PATH   = BASE_DIR / "metadata" / "toc_database.csv"
PDF_DIR    = BASE_DIR / "pdfs"
CHECKPOINT = BASE_DIR / "metadata" / "download_checkpoint.json"

PDF_DIR.mkdir(parents=True, exist_ok=True)

# ── Configuración ──────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (research bot - EAFIT CVP, contacto: cvp@eafit.edu.co)"
}
TIMEOUT   = 30   # segundos por request
DELAY     = 2    # segundos entre descargas (respetar servidores)
MAX_SIZE  = 50 * 1024 * 1024  # 50 MB máximo por PDF


def load_checkpoint() -> dict:
    if CHECKPOINT.exists():
        with open(CHECKPOINT) as f:
            return json.load(f)
    return {}


def save_checkpoint(data: dict):
    with open(CHECKPOINT, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def safe_filename(record: dict) -> str:
    """Genera nombre de archivo seguro desde los metadatos del registro."""
    org   = record.get("organizacion", "org")[:30].replace(" ", "_").replace("/", "-")
    year  = record.get("año", "0000")
    id_   = record.get("id", "000").zfill(3)
    # Quitar caracteres no seguros
    for ch in ['\\', ':', '*', '?', '"', '<', '>', '|', ',', '(', ')']:
        org = org.replace(ch, "")
    return f"{id_}_{year}_{org}.pdf"


def download_pdf(url: str, dest: Path) -> tuple[bool, str]:
    """
    Descarga un PDF desde url a dest.
    Retorna (éxito, mensaje).
    """
    if not url or url.strip() == "":
        return False, "URL vacía"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and "octet-stream" not in content_type.lower():
            # Intentar igualmente si la URL termina en .pdf
            if not url.lower().endswith(".pdf"):
                return False, f"Content-Type no es PDF: {content_type}"

        total = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total += len(chunk)
                    if total > MAX_SIZE:
                        return False, f"Archivo demasiado grande (>{MAX_SIZE//1024//1024} MB)"

        if total < 1000:
            dest.unlink(missing_ok=True)
            return False, f"Archivo demasiado pequeño ({total} bytes), probablemente error"

        return True, f"OK ({total // 1024} KB)"

    except requests.exceptions.ConnectionError:
        return False, "Error de conexión"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP {e.response.status_code}"
    except Exception as e:
        return False, str(e)


def run():
    checkpoint = load_checkpoint()
    results    = []

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    total = len(reader)
    print(f"\n{'='*60}")
    print(f"  DESCARGA DE ToCs — CVP EAFIT")
    print(f"  Total en base de datos: {total} registros")
    print(f"{'='*60}\n")

    for i, row in enumerate(reader, 1):
        id_    = row.get("id", str(i))
        titulo = row.get("titulo", "")[:60]
        url    = row.get("url_pdf", "").strip()
        fname  = safe_filename(row)
        dest   = PDF_DIR / fname

        # Ya descargado anteriormente
        if id_ in checkpoint and checkpoint[id_]["status"] == "ok" and dest.exists():
            print(f"[{i:02d}/{total}] ✓ (ya existe)  {titulo[:55]}")
            continue

        print(f"[{i:02d}/{total}] Descargando   {titulo[:55]}")
        print(f"           URL: {url[:80]}")

        if not url:
            status = "sin_url"
            msg    = "URL de PDF no disponible"
            print(f"           → SKIP: {msg}\n")
        else:
            ok, msg = download_pdf(url, dest)
            status  = "ok" if ok else "error"
            icon    = "✓" if ok else "✗"
            print(f"           → {icon} {msg}\n")
            if ok:
                time.sleep(DELAY)

        checkpoint[id_] = {
            "id":     id_,
            "titulo": titulo,
            "url":    url,
            "file":   fname if status == "ok" else "",
            "status": status,
            "msg":    msg
        }
        save_checkpoint(checkpoint)

    # Resumen
    ok_count   = sum(1 for v in checkpoint.values() if v["status"] == "ok")
    err_count  = sum(1 for v in checkpoint.values() if v["status"] == "error")
    skip_count = sum(1 for v in checkpoint.values() if v["status"] == "sin_url")

    print(f"\n{'='*60}")
    print(f"  RESUMEN FINAL")
    print(f"  Descargados exitosamente : {ok_count}")
    print(f"  Errores de descarga      : {err_count}")
    print(f"  Sin URL de PDF           : {skip_count}")
    print(f"  Total procesados         : {ok_count + err_count + skip_count}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
