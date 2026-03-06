"""
SROI PDF Text Extractor
Descarga PDFs directamente desde las URLs y extrae texto para enriquecer la base de datos.
No requiere almacenamiento local permanente de los PDFs (modo streaming).
"""

import json
import re
import sqlite3
import time
import io
import requests
from pathlib import Path

try:
    from pdfminer.high_level import extract_text_to_fp
    from pdfminer.layout import LAParams
    HAS_PDFMINER = True
except ImportError:
    HAS_PDFMINER = False

try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "sroi_reports.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}
DELAY = 2.0
MAX_PAGES = 15  # Extraer solo las primeras N paginas (resumen ejecutivo)
MAX_SIZE_MB = 30


def extract_pdf_text_from_bytes(pdf_bytes, max_pages=MAX_PAGES):
    """Extrae texto de bytes de PDF."""
    text = ""

    if HAS_PDFMINER:
        try:
            from pdfminer.high_level import extract_text
            text = extract_text(io.BytesIO(pdf_bytes), maxpages=max_pages)
            if text and len(text.strip()) > 50:
                return text[:15000]
        except Exception:
            pass

    if HAS_PYPDF2:
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            pages = reader.pages[:max_pages]
            texts = []
            for page in pages:
                try:
                    texts.append(page.extract_text() or "")
                except Exception:
                    pass
            text = "\n".join(texts)
            if text.strip():
                return text[:15000]
        except Exception:
            pass

    return text


def extract_sroi_metadata_from_text(text):
    """Extrae metadatos estructurados del texto del PDF."""
    if not text:
        return {}

    meta = {}

    # SROI Ratio
    ratio_patterns = [
        r"SROI\s+(?:ratio\s+)?(?:of\s+)?(\d+\.?\d+)",
        r"[£\$](\d+\.?\d+)\s+(?:of\s+(?:social\s+)?value\s+)?(?:for every|per)\s+[£\$]1",
        r"(?:generates?|created?|delivers?)\s+[£\$](\d+\.?\d+)\s+(?:of\s+)?(?:social\s+)?value",
        r"(\d+\.?\d+)\s*:\s*1\s*(?:SROI|ratio)",
        r"return\s+on\s+investment\s+(?:of\s+)?[£\$]?(\d+\.?\d+)",
        r"social\s+return\s+(?:on\s+investment\s+)?(?:ratio\s+)?(?:of\s+)?(\d+\.?\d+)",
    ]

    for pattern in ratio_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1))
                if 0.5 <= val <= 100:
                    meta["sroi_ratio_value"] = val
                    meta["sroi_ratio_raw"] = m.group(0)[:100]
                    break
            except ValueError:
                pass

    # Total investment/cost
    invest_m = re.search(
        r"(?:total\s+)?(?:investment|input|cost)[s]?\s+(?:of\s+|:?\s*)[£\$]?([\d,]+(?:\.\d+)?)",
        text, re.IGNORECASE
    )
    if invest_m:
        meta["total_investment_raw"] = invest_m.group(0)[:100]

    # Total social value
    value_m = re.search(
        r"(?:total\s+)?(?:social\s+)?value\s+(?:created|generated|of)[:\s]+[£\$]?([\d,]+(?:\.\d+)?)",
        text, re.IGNORECASE
    )
    if value_m:
        meta["total_value_raw"] = value_m.group(0)[:100]

    # Periodo de analisis
    period_m = re.search(
        r"(?:period|timeframe|duration)[:\s]+([^\n]{5,60})",
        text, re.IGNORECASE
    )
    if period_m:
        meta["analysis_period"] = period_m.group(1).strip()[:100]

    # Tipo: forecast vs evaluative
    if re.search(r"\bforecast\b", text[:2000], re.IGNORECASE):
        meta["report_type_pdf"] = "Forecast"
    elif re.search(r"\bevaluative?\b", text[:2000], re.IGNORECASE):
        meta["report_type_pdf"] = "Evaluative"

    # Assured
    if re.search(r"\bassured\b", text[:3000], re.IGNORECASE):
        meta["is_assured"] = True

    # Stakeholders count
    stakeholder_m = re.findall(r"\bstakeholder[s]?\b", text, re.IGNORECASE)
    meta["stakeholder_mentions"] = len(stakeholder_m)

    # Extraer primeras 2000 chars como resumen ejecutivo del PDF
    clean_text = re.sub(r"\s+", " ", text).strip()
    meta["pdf_text_extract"] = clean_text[:5000]

    return meta


def process_report_pdfs(db_path, limit=None, delay=DELAY):
    """
    Descarga PDFs y extrae texto para enriquecer la base de datos.
    Procesa solo reportes que tienen pdf_url pero no tienen pdf_text_extract.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Agregar columnas nuevas si no existen
    new_columns = [
        "pdf_text_extract TEXT",
        "sroi_ratio_pdf REAL",
        "total_investment_raw TEXT",
        "total_value_raw TEXT",
        "analysis_period TEXT",
        "report_type_pdf TEXT",
        "is_assured INTEGER DEFAULT 0",
        "stakeholder_mentions INTEGER",
        "pdf_pages INTEGER",
        "pdf_extracted INTEGER DEFAULT 0",
    ]
    for col_def in new_columns:
        col_name = col_def.split()[0]
        try:
            conn.execute(f"ALTER TABLE reports ADD COLUMN {col_def}")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Columna ya existe

    # Obtener reportes con PDF no procesados
    query = """
        SELECT id, slug, pdf_url FROM reports
        WHERE pdf_url IS NOT NULL
        AND (pdf_extracted IS NULL OR pdf_extracted = 0)
        ORDER BY id
    """
    if limit:
        query += f" LIMIT {limit}"

    reports = conn.execute(query).fetchall()
    print(f"Reports to process: {len(reports)}")

    processed = 0
    errors = 0

    for i, report in enumerate(reports):
        report_id = report["id"]
        pdf_url = report["pdf_url"]
        slug = report["slug"]

        print(f"[{i+1}/{len(reports)}] {slug[:50]}")

        try:
            # Descargar PDF
            resp = requests.get(pdf_url, headers=HEADERS, stream=True, timeout=60)
            if resp.status_code == 404:
                conn.execute("UPDATE reports SET pdf_extracted=2 WHERE id=?", (report_id,))
                conn.commit()
                continue

            resp.raise_for_status()

            # Verificar tamano
            content_length = int(resp.headers.get("Content-Length", 0))
            if content_length > MAX_SIZE_MB * 1024 * 1024:
                print(f"  -> Skipping: too large ({content_length/1024/1024:.1f}MB)")
                conn.execute("UPDATE reports SET pdf_extracted=3 WHERE id=?", (report_id,))
                conn.commit()
                continue

            # Leer PDF en memoria
            pdf_bytes = b""
            for chunk in resp.iter_content(chunk_size=65536):
                pdf_bytes += chunk
                if len(pdf_bytes) > MAX_SIZE_MB * 1024 * 1024:
                    break

            # Extraer texto
            text = extract_pdf_text_from_bytes(pdf_bytes)
            if not text or len(text.strip()) < 50:
                print(f"  -> No text extracted")
                conn.execute("UPDATE reports SET pdf_extracted=4 WHERE id=?", (report_id,))
                conn.commit()
                errors += 1
                time.sleep(0.5)
                continue

            # Extraer metadatos del texto
            meta = extract_sroi_metadata_from_text(text)
            print(f"  -> {len(text)} chars | SROI: {meta.get('sroi_ratio_value', 'N/A')}")

            # Actualizar base de datos
            conn.execute("""
                UPDATE reports SET
                    pdf_text_extract = :pdf_text_extract,
                    sroi_ratio_pdf = :sroi_ratio_pdf,
                    sroi_ratio_value = COALESCE(:sroi_ratio_pdf, sroi_ratio_value),
                    total_investment_raw = :total_investment_raw,
                    total_value_raw = :total_value_raw,
                    analysis_period = :analysis_period,
                    report_type_pdf = :report_type_pdf,
                    is_assured = :is_assured,
                    stakeholder_mentions = :stakeholder_mentions,
                    pdf_extracted = 1
                WHERE id = :id
            """, {
                "id": report_id,
                "pdf_text_extract": meta.get("pdf_text_extract"),
                "sroi_ratio_pdf": meta.get("sroi_ratio_value"),
                "total_investment_raw": meta.get("total_investment_raw"),
                "total_value_raw": meta.get("total_value_raw"),
                "analysis_period": meta.get("analysis_period"),
                "report_type_pdf": meta.get("report_type_pdf"),
                "is_assured": 1 if meta.get("is_assured") else 0,
                "stakeholder_mentions": meta.get("stakeholder_mentions", 0),
            })
            conn.commit()
            processed += 1

        except Exception as e:
            print(f"  -> ERROR: {e}")
            errors += 1

        time.sleep(delay)

    conn.close()
    print(f"\nDone: {processed} processed | {errors} errors")


def main():
    if not DB_PATH.exists():
        print(f"ERROR: Run 03_build_database.py first.")
        return

    if not HAS_PDFMINER and not HAS_PYPDF2:
        print("ERROR: Install pdfminer.six or PyPDF2: pip install pdfminer.six")
        return

    print(f"PDF extraction using: {'pdfminer' if HAS_PDFMINER else 'PyPDF2'}")
    process_report_pdfs(DB_PATH)

    # Re-exportar para el agente con los nuevos datos
    conn = sqlite3.connect(DB_PATH)
    output_path = DATA_DIR / "sroi_reports_for_agent.jsonl"
    rows = conn.execute("""
        SELECT * FROM reports WHERE error IS NULL ORDER BY publication_year DESC NULLS LAST, id
    """).fetchall()
    cols = [desc[0] for desc in conn.execute("SELECT * FROM reports LIMIT 0").description]

    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            record = {k: v for k, v in zip(cols, row) if v is not None}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"Re-exported {len(rows)} reports with PDF text to {output_path}")
    conn.close()


if __name__ == "__main__":
    main()
