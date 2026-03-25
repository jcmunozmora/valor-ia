"""
02_extract_toc_text.py
----------------------
Extrae texto de los PDFs descargados y busca elementos clave de la ToC:
  - Cadena causal (inputs → actividades → outputs → outcomes → impacto)
  - Supuestos (assumptions)
  - Grupos de stakeholders
  - Indicadores mencionados
  - Sector / contexto geográfico

Genera: metadata/toc_extracted.jsonl (un JSON por línea)

Dependencias: pdfminer.six, pandas
Instalar: pip install pdfminer.six pandas
"""

import csv
import json
import re
from pathlib import Path

try:
    from pdfminer.high_level import extract_text
    HAS_PDFMINER = True
except ImportError:
    try:
        import pypdf
        HAS_PDFMINER = False
    except ImportError:
        raise ImportError("Instalar pdfminer.six o pypdf: pip install pdfminer.six")

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.parent
CSV_PATH    = BASE_DIR / "metadata" / "toc_database.csv"
PDF_DIR     = BASE_DIR / "pdfs"
CHECKPOINT  = BASE_DIR / "metadata" / "download_checkpoint.json"
OUTPUT_JSONL = BASE_DIR / "metadata" / "toc_extracted.jsonl"

# ── Palabras clave para detección de elementos ToC ────────────────────────────
TOC_PATTERNS = {
    "inputs":      r"\b(inputs?|recursos?|inversión|funding|budget|presupuesto)\b",
    "activities":  r"\b(activit|actividades|interventions?|program|programa|estrategia)\b",
    "outputs":     r"\b(outputs?|productos?|deliverables?|entregables?)\b",
    "outcomes":    r"\b(outcomes?|resultados?|cambios?|beneficios?|efectos?)\b",
    "impact":      r"\b(impact[oa]?s?|impacto|long.term|largo plazo|transformaci[oó]n)\b",
    "assumptions": r"\b(assumptions?|supuestos?|asumiendo|hipótesis|condiciones)\b",
    "indicators":  r"\b(indicators?|indicadores?|métricas?|medición|measurement)\b",
    "stakeholders":r"\b(stakeholders?|actores?|beneficiarios?|grupos?|comunidad|participantes)\b",
}

SECTOR_KEYWORDS = {
    "salud":         r"\b(health|salud|medical|médico|clinic|hospital)\b",
    "salud_mental":  r"\b(mental health|salud mental|wellbeing|bienestar|psycho)\b",
    "educacion":     r"\b(education|educac|school|escuela|aprendizaje|learning)\b",
    "empleo":        r"\b(employment|empleo|workforce|trabajo|job|capacitación)\b",
    "nutricion":     r"\b(nutrition|nutrición|food security|seguridad alimentaria|hunger|hambre)\b",
    "vivienda":      r"\b(housing|vivienda|shelter|hábitat|habitación)\b",
    "clima":         r"\b(climate|clima|environment|medio ambiente|resilience|resiliencia)\b",
    "sroi":          r"\b(sroi|social return|retorno social|impact map|mapa de impacto)\b",
    "gobernanza":    r"\b(governance|gobernanza|democracy|democracia|anticorruption)\b",
    "genero":        r"\b(gender|género|women|mujer|feminist|feminista)\b",
}


def extract_pdf_text(pdf_path: Path, max_pages: int = 15) -> str:
    """Extrae texto de las primeras N páginas del PDF."""
    try:
        if HAS_PDFMINER:
            text = extract_text(str(pdf_path), maxpages=max_pages)
        else:
            reader = pypdf.PdfReader(str(pdf_path))
            pages  = min(max_pages, len(reader.pages))
            text   = "\n".join(
                reader.pages[i].extract_text() or "" for i in range(pages)
            )
        return text or ""
    except Exception as e:
        return f"[ERROR: {e}]"


def detect_elements(text: str) -> dict:
    """Detecta qué elementos de ToC están presentes en el texto."""
    text_lower = text.lower()
    found = {}
    for key, pattern in TOC_PATTERNS.items():
        matches = re.findall(pattern, text_lower, flags=re.IGNORECASE)
        found[f"has_{key}"]    = len(matches) > 0
        found[f"count_{key}"]  = len(matches)
    return found


def detect_sectors(text: str) -> list:
    """Detecta sectores mencionados en el texto."""
    text_lower = text.lower()
    sectors = []
    for sector, pattern in SECTOR_KEYWORDS.items():
        if re.search(pattern, text_lower, flags=re.IGNORECASE):
            sectors.append(sector)
    return sectors


def extract_snippet(text: str, keyword: str, window: int = 300) -> str:
    """Extrae un fragmento de texto alrededor de un keyword."""
    idx = text.lower().find(keyword.lower())
    if idx == -1:
        return ""
    start = max(0, idx - window // 2)
    end   = min(len(text), idx + window // 2)
    return text[start:end].strip().replace("\n", " ")


def run():
    # Cargar checkpoint de descargas
    if not CHECKPOINT.exists():
        print("No se encontró checkpoint de descargas. Ejecutar 01_download_tocs.py primero.")
        return

    with open(CHECKPOINT) as f:
        checkpoint = json.load(f)

    # Cargar base de datos original
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        db = {row["id"]: row for row in csv.DictReader(f)}

    downloaded = {
        id_: info for id_, info in checkpoint.items()
        if info["status"] == "ok" and info.get("file")
    }

    print(f"\n{'='*60}")
    print(f"  EXTRACCIÓN DE TEXTO — ToC DATABASE")
    print(f"  PDFs disponibles: {len(downloaded)}")
    print(f"{'='*60}\n")

    with open(OUTPUT_JSONL, "w", encoding="utf-8") as out:
        for i, (id_, info) in enumerate(downloaded.items(), 1):
            pdf_path = PDF_DIR / info["file"]
            if not pdf_path.exists():
                continue

            meta = db.get(id_, {})
            print(f"[{i:02d}/{len(downloaded)}] Procesando: {info['titulo'][:55]}")

            text = extract_pdf_text(pdf_path, max_pages=15)
            if text.startswith("[ERROR"):
                print(f"           → Error: {text}\n")
                continue

            elements = detect_elements(text)
            sectors  = detect_sectors(text)

            # Snippets clave
            snippets = {}
            for kw in ["theory of change", "teoría del cambio", "impact map",
                        "outcomes", "assumptions", "supuestos"]:
                s = extract_snippet(text, kw, window=400)
                if s:
                    snippets[kw] = s

            record = {
                "id":           id_,
                "titulo":       meta.get("titulo", ""),
                "organizacion": meta.get("organizacion", ""),
                "tipo_org":     meta.get("tipo_org", ""),
                "sector":       meta.get("sector", ""),
                "subsector":    meta.get("subsector", ""),
                "pais":         meta.get("pais_contexto", ""),
                "año":          meta.get("año", ""),
                "idioma":       meta.get("idioma", ""),
                "nivel_toc":    meta.get("nivel_toc", ""),
                "url":          meta.get("url", ""),
                "file":         info["file"],
                "text_chars":   len(text),
                **elements,
                "sectors_detected": sectors,
                "snippets":     snippets,
            }

            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            print(f"           → {len(text):,} chars | elementos: {sum(1 for k,v in elements.items() if k.startswith('has_') and v)}/8 | sectores: {sectors}\n")

    print(f"\nResultado guardado en: {OUTPUT_JSONL}")


if __name__ == "__main__":
    run()
