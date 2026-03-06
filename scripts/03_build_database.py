"""
SROI Database Builder
Crea una base de datos SQLite estructurada a partir de los metadatos scrapeados.
Optimizada para uso como base de conocimiento del agente SROI.
"""

import json
import sqlite3
import csv
from pathlib import Path
from datetime import datetime
import re

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "sroi_reports.db"


def clean_sroi_ratio(ratio_text):
    """Extrae el valor numerico del ratio SROI."""
    if not ratio_text:
        return None
    text = str(ratio_text)
    # Solo aceptar si parece un ratio real (numero entre 0.5 y 50)
    # Patrones: "4.57", "£4.57 per £1", "$3.20 for every $1"
    for pattern in [
        r"[£\$](\d+\.?\d+)\s+(?:for every|per)\s+[£\$]1",
        r"SROI\s+(?:ratio\s+)?(?:of\s+)?(\d+\.?\d+)",
        r"(?:ratio|return)\s+(?:is\s+)?(\d+\.?\d+)",
        r"(\d+\.?\d+)\s*:\s*1",
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1))
                if 0.5 <= val <= 100:
                    return val
            except ValueError:
                pass
    return None


def extract_sroi_ratio_from_text(full_text):
    """Busca ratio SROI en el texto completo del reporte."""
    if not full_text:
        return None
    patterns = [
        r"SROI\s+(?:ratio\s+)?(?:of\s+)?(\d+\.?\d+)",
        r"[£\$](\d+\.?\d+)\s+(?:of value\s+)?(?:for every|per)\s+[£\$]1",
        r"(?:generates?|created?|delivers?)\s+[£\$]?(\d+\.?\d+)\s+(?:of\s+)?(?:social\s+)?value",
        r"(\d+\.?\d+)\s*:\s*1\s*(?:SROI|ratio|return)",
        r"SROI\s+(?:of\s+)?(\d+\.?\d+)(?:\s|;|,|\.)",
    ]
    for pattern in patterns:
        m = re.search(pattern, full_text, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1))
                if 0.5 <= val <= 100:
                    return val
            except ValueError:
                pass
    return None


def normalize_year(year_text):
    """Extrae anio de publicacion."""
    if not year_text:
        return None
    match = re.search(r"(20\d{2}|19\d{2})", str(year_text))
    return int(match.group(1)) if match else None


def extract_year_from_date(date_str):
    """Extrae anio de una fecha ISO o texto."""
    if not date_str:
        return None
    match = re.search(r"(20\d{2}|19\d{2})", str(date_str))
    return int(match.group(1)) if match else None


def create_schema(conn):
    """Crea el esquema de la base de datos."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE NOT NULL,
            url TEXT NOT NULL,
            title TEXT,
            abstract TEXT,
            full_text TEXT,
            published_date TEXT,
            publication_year INTEGER,
            organization TEXT,
            country TEXT,
            sector TEXT,
            report_type TEXT,
            assurance_status TEXT,
            sroi_ratio_raw TEXT,
            sroi_ratio_value REAL,
            pdf_url TEXT,
            pdf_local_path TEXT,
            thumbnail_url TEXT,
            has_pdf INTEGER DEFAULT 0,
            scraped_at TEXT,
            error TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS report_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER REFERENCES reports(id),
            category TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS report_pdf_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER REFERENCES reports(id),
            pdf_url TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_reports_slug ON reports(slug);
        CREATE INDEX IF NOT EXISTS idx_reports_year ON reports(publication_year);
        CREATE INDEX IF NOT EXISTS idx_reports_country ON reports(country);
        CREATE INDEX IF NOT EXISTS idx_reports_sroi ON reports(sroi_ratio_value);
        CREATE INDEX IF NOT EXISTS idx_reports_has_pdf ON reports(has_pdf);

        -- Vista para el agente: reportes con informacion completa
        CREATE VIEW IF NOT EXISTS v_complete_reports AS
        SELECT
            r.*,
            GROUP_CONCAT(DISTINCT rc.category) as all_categories
        FROM reports r
        LEFT JOIN report_categories rc ON rc.report_id = r.id
        GROUP BY r.id;
    """)
    conn.commit()


def load_pdf_log(data_dir):
    """Carga el log de descargas de PDF para enriquecer datos."""
    log_path = data_dir / "pdf_download_log.json"
    if not log_path.exists():
        return {}
    with open(log_path, "r", encoding="utf-8") as f:
        log = json.load(f)
    return {item["slug"]: item for item in log}


def insert_reports(conn, reports, pdf_log):
    """Inserta reportes en la base de datos."""
    inserted = 0
    updated = 0
    skipped = 0

    for report in reports:
        slug = report.get("slug", "")
        if not slug:
            skipped += 1
            continue

        # Enriquecer con info de PDF descargado
        pdf_info = pdf_log.get(slug, {})
        pdf_local = pdf_info.get("local_path")

        # Anio de publicacion
        pub_year = extract_year_from_date(report.get("published_date")) or \
                   normalize_year(report.get("year"))

        # Extraer abstract real del full_text si el meta abstract es generico
        abstract = report.get("abstract") or ""
        if not abstract or abstract.startswith("Read our report:") or len(abstract) < 80:
            ft = report.get("full_text", "")
            # Intentar extraer del full_text despues del titulo o de "Abstract"
            import re as _re
            # Buscar "Abstract\n" en full_text
            abs_match = _re.search(r"Abstract\n([\s\S]{50,2000}?)(?:\nDownload|\nView|\nRelated|\Z)", ft)
            if abs_match:
                abstract = abs_match.group(1).strip()[:1500]
            elif ft:
                # Tomar el contenido sustancial (despues del titulo, si hay mas de 200 chars)
                title = report.get("title", "")
                if title and title in ft:
                    remainder = ft[ft.find(title) + len(title):].strip()
                    if len(remainder) > 100:
                        abstract = remainder[:1500]
                elif len(ft) > 200:
                    abstract = ft[:1500]

        row = {
            "slug": slug,
            "url": report.get("url", ""),
            "title": report.get("title"),
            "abstract": abstract if abstract else None,
            "full_text": report.get("full_text"),
            "published_date": report.get("published_date"),
            "publication_year": pub_year,
            "organization": report.get("organization"),
            "country": report.get("country"),
            "sector": report.get("sector"),
            "report_type": report.get("report_type"),
            "assurance_status": report.get("assurance"),
            "sroi_ratio_raw": report.get("sroi_ratio"),
            "sroi_ratio_value": (
                clean_sroi_ratio(report.get("sroi_ratio")) or
                extract_sroi_ratio_from_text(report.get("full_text")) or
                extract_sroi_ratio_from_text(abstract)
            ),
            "pdf_url": report.get("pdf_url"),
            "pdf_local_path": pdf_local,
            "thumbnail_url": report.get("thumbnail_url"),
            "has_pdf": 1 if (report.get("pdf_url") or pdf_local) else 0,
            "scraped_at": report.get("scraped_at"),
            "error": report.get("error"),
        }

        try:
            conn.execute("""
                INSERT INTO reports
                    (slug, url, title, abstract, full_text, published_date, publication_year,
                     organization, country, sector, report_type, assurance_status,
                     sroi_ratio_raw, sroi_ratio_value, pdf_url, pdf_local_path,
                     thumbnail_url, has_pdf, scraped_at, error)
                VALUES
                    (:slug, :url, :title, :abstract, :full_text, :published_date, :publication_year,
                     :organization, :country, :sector, :report_type, :assurance_status,
                     :sroi_ratio_raw, :sroi_ratio_value, :pdf_url, :pdf_local_path,
                     :thumbnail_url, :has_pdf, :scraped_at, :error)
                ON CONFLICT(slug) DO UPDATE SET
                    title = excluded.title,
                    abstract = excluded.abstract,
                    full_text = excluded.full_text,
                    publication_year = excluded.publication_year,
                    organization = excluded.organization,
                    country = excluded.country,
                    sector = excluded.sector,
                    sroi_ratio_value = excluded.sroi_ratio_value,
                    pdf_local_path = excluded.pdf_local_path,
                    has_pdf = excluded.has_pdf
            """, row)

            # Obtener el ID del reporte
            report_id = conn.execute("SELECT id FROM reports WHERE slug=?", (slug,)).fetchone()[0]

            # Insertar categorias
            categories = report.get("categories", [])
            if categories:
                conn.execute("DELETE FROM report_categories WHERE report_id=?", (report_id,))
                for cat in categories:
                    if cat:
                        conn.execute("INSERT INTO report_categories (report_id, category) VALUES (?,?)",
                                     (report_id, cat))

            # Insertar todos los PDF URLs
            pdf_urls = report.get("pdf_urls", [])
            if pdf_urls:
                conn.execute("DELETE FROM report_pdf_urls WHERE report_id=?", (report_id,))
                for pdf_url in pdf_urls:
                    conn.execute("INSERT INTO report_pdf_urls (report_id, pdf_url) VALUES (?,?)",
                                 (report_id, pdf_url))

            inserted += 1

        except Exception as e:
            print(f"  ERROR inserting {slug}: {e}")
            skipped += 1

    conn.commit()
    return inserted, updated, skipped


def print_stats(conn):
    """Imprime estadisticas de la base de datos."""
    total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
    with_pdf = conn.execute("SELECT COUNT(*) FROM reports WHERE has_pdf=1").fetchone()[0]
    with_ratio = conn.execute("SELECT COUNT(*) FROM reports WHERE sroi_ratio_value IS NOT NULL").fetchone()[0]
    with_abstract = conn.execute("SELECT COUNT(*) FROM reports WHERE abstract IS NOT NULL").fetchone()[0]
    errors = conn.execute("SELECT COUNT(*) FROM reports WHERE error IS NOT NULL").fetchone()[0]

    print(f"\n{'='*50}")
    print(f"DATABASE STATS")
    print(f"{'='*50}")
    print(f"Total reports:      {total}")
    print(f"With PDF:           {with_pdf} ({100*with_pdf//max(total,1)}%)")
    print(f"With SROI ratio:    {with_ratio} ({100*with_ratio//max(total,1)}%)")
    print(f"With abstract:      {with_abstract} ({100*with_abstract//max(total,1)}%)")
    print(f"With errors:        {errors}")

    # Por anio
    print(f"\nBy year (top 10):")
    for row in conn.execute("""
        SELECT publication_year, COUNT(*) as n
        FROM reports WHERE publication_year IS NOT NULL
        GROUP BY publication_year ORDER BY publication_year DESC LIMIT 10
    """):
        print(f"  {row[0]}: {row[1]}")

    # Por pais
    print(f"\nBy country (top 10):")
    for row in conn.execute("""
        SELECT country, COUNT(*) as n
        FROM reports WHERE country IS NOT NULL
        GROUP BY country ORDER BY n DESC LIMIT 10
    """):
        print(f"  {row[0]}: {row[1]}")


def export_for_agent(conn, data_dir):
    """
    Exporta un archivo JSONL optimizado para entrenamiento/RAG del agente.
    Cada linea es un reporte completo para ingesta facil.
    """
    output_path = data_dir / "sroi_reports_for_agent.jsonl"
    rows = conn.execute("""
        SELECT r.*, GROUP_CONCAT(DISTINCT rc.category) as categories
        FROM reports r
        LEFT JOIN report_categories rc ON rc.report_id = r.id
        WHERE r.error IS NULL
        GROUP BY r.id
        ORDER BY r.publication_year DESC NULLS LAST, r.id
    """).fetchall()

    cols = [desc[0] for desc in conn.execute("SELECT * FROM reports LIMIT 0").description]
    cols_with_cat = cols + ["categories"]

    with open(output_path, "w", encoding="utf-8") as f:
        for row in rows:
            record = dict(zip(cols_with_cat, row))
            # Limpiar None
            record = {k: v for k, v in record.items() if v is not None}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"\nExported {len(rows)} reports for agent training: {output_path}")


def main():
    metadata_path = DATA_DIR / "reports_metadata.json"
    if not metadata_path.exists():
        print(f"ERROR: Run 01_scrape_reports.py first.")
        return

    with open(metadata_path, "r", encoding="utf-8") as f:
        reports = json.load(f)
    print(f"Loaded {len(reports)} reports from JSON")

    pdf_log = load_pdf_log(DATA_DIR)
    print(f"Loaded PDF log: {len(pdf_log)} entries")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    create_schema(conn)

    inserted, updated, skipped = insert_reports(conn, reports, pdf_log)
    print(f"Inserted: {inserted} | Skipped: {skipped}")

    print_stats(conn)
    export_for_agent(conn, DATA_DIR)

    conn.close()
    print(f"\nDatabase saved: {DB_PATH}")


if __name__ == "__main__":
    main()
