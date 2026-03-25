"""
04_build_summary.py
--------------------
Genera un reporte resumen del estado del repositorio de ToCs.
Salida: metadata/summary_report.md y metadata/stats.json

Dependencias: pandas (opcional, usa csv si no está disponible)
"""

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

BASE_DIR   = Path(__file__).parent.parent
CSV_PATH   = BASE_DIR / "metadata" / "toc_database.csv"
JSONL_PATH = BASE_DIR / "metadata" / "toc_extracted.jsonl"
CHECKPOINT = BASE_DIR / "metadata" / "download_checkpoint.json"
SUMMARY_MD = BASE_DIR / "metadata" / "summary_report.md"
STATS_JSON = BASE_DIR / "metadata" / "stats.json"


def load_db() -> list[dict]:
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_extracted() -> list[dict]:
    if not JSONL_PATH.exists():
        return []
    records = []
    with open(JSONL_PATH) as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return records


def load_checkpoint() -> dict:
    if not CHECKPOINT.exists():
        return {}
    with open(CHECKPOINT) as f:
        return json.load(f)


def run():
    db         = load_db()
    extracted  = load_extracted()
    checkpoint = load_checkpoint()

    total      = len(db)
    downloaded = sum(1 for v in checkpoint.values() if v["status"] == "ok")
    errors     = sum(1 for v in checkpoint.values() if v["status"] == "error")
    no_url     = sum(1 for v in checkpoint.values() if v["status"] == "sin_url")
    n_text     = len(extracted)

    # Distribuciones
    by_sector  = Counter(r.get("sector", "Desconocido") for r in db)
    by_idioma  = Counter(r.get("idioma", "?") for r in db)
    by_tipo    = Counter(r.get("tipo_org", "?") for r in db)
    by_pais    = Counter(r.get("pais_contexto", "?") for r in db)
    by_nivel   = Counter(r.get("nivel_toc", "?") for r in db)

    stats = {
        "fecha_generacion": datetime.now().isoformat(),
        "total_registros":  total,
        "pdfs_descargados": downloaded,
        "errores_descarga": errors,
        "sin_url_pdf":      no_url,
        "con_texto_extraido": n_text,
        "por_sector":  dict(by_sector.most_common()),
        "por_idioma":  dict(by_idioma.most_common()),
        "por_tipo_org": dict(by_tipo.most_common()),
        "por_pais":    dict(by_pais.most_common()),
        "por_nivel_toc": dict(by_nivel.most_common()),
    }

    with open(STATS_JSON, "w") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    # ToCs con texto extraído: elementos detectados
    elements_summary = defaultdict(int)
    for r in extracted:
        for k in ["has_inputs", "has_activities", "has_outputs", "has_outcomes",
                  "has_impact", "has_assumptions", "has_indicators", "has_stakeholders"]:
            if r.get(k):
                elements_summary[k] += 1

    # Markdown report
    lines = [
        f"# Repositorio ToC — CVP EAFIT",
        f"",
        f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"",
        f"## Resumen estadístico",
        f"",
        f"| Métrica | Valor |",
        f"|---------|-------|",
        f"| Total de registros en base de datos | **{total}** |",
        f"| PDFs descargados exitosamente | **{downloaded}** ({downloaded*100//max(total,1)}%) |",
        f"| Con texto extraído y analizado | **{n_text}** |",
        f"| Errores de descarga | {errors} |",
        f"| Sin URL de PDF disponible | {no_url} |",
        f"",
        f"## Distribución por sector",
        f"",
    ]
    for sector, count in by_sector.most_common():
        lines.append(f"- **{sector}**: {count}")

    lines += ["", "## Distribución por idioma", ""]
    for idioma, count in by_idioma.most_common():
        lines.append(f"- **{idioma}**: {count}")

    lines += ["", "## Distribución por tipo de organización", ""]
    for tipo, count in by_tipo.most_common():
        lines.append(f"- **{tipo}**: {count}")

    lines += ["", "## Elementos de ToC detectados en PDFs analizados", ""]
    element_labels = {
        "has_inputs": "Inputs / Recursos",
        "has_activities": "Actividades / Intervenciones",
        "has_outputs": "Outputs / Productos",
        "has_outcomes": "Outcomes / Resultados",
        "has_impact": "Impacto de largo plazo",
        "has_assumptions": "Supuestos / Assumptions",
        "has_indicators": "Indicadores",
        "has_stakeholders": "Stakeholders / Beneficiarios",
    }
    for key, label in element_labels.items():
        count = elements_summary.get(key, 0)
        pct   = count * 100 // max(n_text, 1)
        lines.append(f"- **{label}**: {count}/{n_text} ({pct}%)")

    lines += [
        "",
        "## Lista completa de registros",
        "",
        "| ID | Título | Organización | Sector | Idioma | Estado |",
        "|----|--------|-------------|--------|--------|--------|",
    ]
    for r in db:
        status = checkpoint.get(r["id"], {}).get("status", "pendiente")
        icon   = "✓" if status == "ok" else ("✗" if status == "error" else "⏳")
        lines.append(
            f"| {r['id']} | {r['titulo'][:50]} | {r['organizacion'][:30]} "
            f"| {r['sector'][:20]} | {r['idioma']} | {icon} {status} |"
        )

    with open(SUMMARY_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Imprimir resumen en consola
    print(f"\n{'='*60}")
    print(f"  REPOSITORIO ToC — RESUMEN")
    print(f"{'='*60}")
    print(f"  Total registros     : {total}")
    print(f"  PDFs descargados    : {downloaded} ({downloaded*100//max(total,1)}%)")
    print(f"  Texto extraído      : {n_text}")
    print()
    print(f"  Sectores:")
    for s, c in by_sector.most_common(10):
        print(f"    {'·':>2} {s:<35} {c}")
    print()
    print(f"  Reporte: {SUMMARY_MD}")
    print(f"  Stats:   {STATS_JSON}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
