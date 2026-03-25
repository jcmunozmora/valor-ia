"""
05_maturity_index.py
---------------------
Índice de Madurez de la Teoría del Cambio (IMToC)
CVP EAFIT — Centro Valor Público

El IMToC califica cada ToC en 10 dimensiones agrupadas en 3 dominios:

DOMINIO A — Arquitectura causal (máx 30 pts)
  A1. Cadena causal completa         (0–10)
  A2. Especificidad de outcomes      (0–10)
  A3. Diferenciación por stakeholder (0–10)

DOMINIO B — Robustez epistémica (máx 40 pts)
  B1. Supuestos documentados         (0–10)
  B2. Base de evidencia              (0–10)
  B3. Indicadores definidos          (0–10)
  B4. Dimensión temporal             (0–10)

DOMINIO C — Completitud y uso (máx 30 pts)
  C1. Involucramiento de stakeholders (0–10)
  C2. Outcomes negativos / riesgos    (0–10)
  C3. Claridad y comunicación         (0–10)

Total: 0–100 pts → 5 niveles de madurez (N1–N5)

Salidas:
  - metadata/imtoc_scores.csv     ← puntuaciones por dimensión
  - metadata/imtoc_report.md      ← reporte en markdown
  - metadata/imtoc_data.json      ← datos para visualización
"""

import re
import csv
import json
from pathlib import Path
from collections import defaultdict

# ── Rutas ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).parent.parent
JSONL_PATH    = BASE_DIR / "metadata" / "toc_extracted.jsonl"
CSV_DB        = BASE_DIR / "metadata" / "toc_database.csv"
SCORES_CSV    = BASE_DIR / "metadata" / "imtoc_scores.csv"
REPORT_MD     = BASE_DIR / "metadata" / "imtoc_report.md"
DATA_JSON     = BASE_DIR / "metadata" / "imtoc_data.json"

# ── Niveles de madurez ────────────────────────────────────────────────────────
MATURITY_LEVELS = [
    (0,  20,  "N1", "Proto-ToC",      "Solo intenciones o lista de actividades. Sin cadena causal explícita."),
    (21, 40,  "N2", "ToC Básica",     "Cadena causal incompleta. Outcomes vagos. Sin supuestos documentados."),
    (41, 60,  "N3", "ToC Desarrollada","Cadena causal clara. Outcomes identificados. Supuestos parciales."),
    (61, 80,  "N4", "ToC Robusta",    "Todos los elementos presentes. Evidencia parcial. Indicadores definidos."),
    (81, 100, "N5", "ToC de Excelencia","Completa, evidenciada, co-construida, con indicadores y temporalidad."),
]

# ── Patrones de scoring ───────────────────────────────────────────────────────

# A1 — Cadena causal completa
# Evalúa presencia de cada eslabón de la cadena
CHAIN_ELEMENTS = {
    "inputs":       [r"\binputs?\b", r"\brecurs[oa]s?\b", r"\binversión\b", r"\bfunding\b", r"\bbudget\b"],
    "activities":   [r"\bactivit", r"\bintervenc", r"\bactividades\b", r"\bprograma\b", r"\bacciones\b"],
    "outputs":      [r"\boutputs?\b", r"\bproductos?\b", r"\bentregables?\b", r"\bbienes\b"],
    "outcomes":     [r"\boutcomes?\b", r"\bresultados?\b", r"\bcambios?\b", r"\bbeneficio", r"\befecto"],
    "impact":       [r"\bimpact[oa]?\b", r"\blong.?term\b", r"\blargo plazo\b", r"\btransformaci"],
}

# A2 — Especificidad de outcomes
SPECIFIC_OUTCOMES = [
    r"\b(specific|específic)\b",
    r"\b(measur|medible|cuantific)\b",
    r"\b(short.?term|medium.?term|long.?term|corto plazo|mediano plazo)\b",
    r"\b(outcome statement|declaración de resultado)\b",
    r"\b(behavior|actitud|conocimiento|habilidad|skill)\b",
]

# A3 — Diferenciación por stakeholder
STAKEHOLDER_DIFF = [
    r"\b(beneficiari[oa]s?\b.{0,50}(outcome|resultado|cambio))",
    r"\b(group|grupo|comunidad|familia|mujer|youth|joven|niño|adult)\b",
    r"\b(different.{0,20}stakeholder|distintos.{0,20}actor)",
    r"\b(primary|secondary).{0,20}(beneficiar|stakeholder)",
    r"\b(participant|participante|usuario|client)\b",
]

# B1 — Supuestos documentados
ASSUMPTIONS_PATTERNS = [
    r"\bassumptions?\b",
    r"\bsupuestos?\b",
    r"\bif.{0,30}then\b",
    r"\bsi.{0,30}entonces\b",
    r"\b(pre.?condition|condición previa|condición necesaria)\b",
    r"\b(implicit|explicit).{0,20}(assumption|supuesto)\b",
    r"\b(enabling condition|condición habilitante)\b",
]

# B2 — Base de evidencia
EVIDENCE_PATTERNS = [
    r"\b(evidence|evidencia|research|investigación)\b",
    r"\b(study|estudios?|literature|literatura)\b",
    r"\b(data|datos|statistics|estadísticas)\b",
    r"\b(evaluation|evaluación|assessment)\b",
    r"\b(randomized|aleatorizad|RCT|experimental)\b",
    r"\b(systematic review|revisión sistemática)\b",
    r"\b(causal|causalidad|attribution|atribución)\b",
]

# B3 — Indicadores definidos
INDICATORS_PATTERNS = [
    r"\bindicators?\b",
    r"\bindicadores?\b",
    r"\b(metric|métrica|measure|medida)\b",
    r"\b(baseline|línea de base|línea base)\b",
    r"\b(target|meta|benchmark)\b",
    r"\b(survey|encuesta|scale|escala)\b",
    r"\b(SMART|específico.{0,10}medible)\b",
]

# B4 — Dimensión temporal
TEMPORAL_PATTERNS = [
    r"\b(short.?term|immediate|corto plazo|inmediato)\b",
    r"\b(medium.?term|mediano plazo|intermediate)\b",
    r"\b(long.?term|largo plazo|sustained)\b",
    r"\b(duration|duración|timeline|cronograma)\b",
    r"\b(year|años?|months|meses)\b",
    r"\b(drop.?off|decay|fade)\b",
]

# C1 — Involucramiento de stakeholders
PARTICIPATION_PATTERNS = [
    r"\b(participat|participaci)\b",
    r"\b(co.?creat|co.?design|co.?construct)\b",
    r"\b(consulta|consultation|workshop|taller)\b",
    r"\b(community.?led|community.?based)\b",
    r"\b(voice|voz|ownership|apropiación)\b",
    r"\b(stakeholder.{0,20}(engaged|involucrado|consulted))\b",
]

# C2 — Outcomes negativos / riesgos
NEGATIVE_PATTERNS = [
    r"\b(unintended|no.?intencional|unexpected)\b",
    r"\b(negative.{0,20}(outcome|effect|impact)|outcome.{0,20}negative)\b",
    r"\b(risk|riesgo|threat|amenaza)\b",
    r"\b(barrier|barrera|obstacle|obstáculo)\b",
    r"\b(displacement|desplazamiento)\b",
    r"\b(harm|daño|adverse)\b",
]

# C3 — Claridad y comunicación
CLARITY_PATTERNS = [
    r"\b(diagram|diagrama|map|mapa|chart|gráfico)\b",
    r"\b(visual|figure|figura|illustration)\b",
    r"\b(narrative|narrativa|story|historia)\b",
    r"\b(logic model|modelo lógico|logframe)\b",
    r"\b(clear|claro|explicit|explícito)\b",
    r"\b(communicate|comunicar|share|compartir)\b",
]

ALL_DIMENSIONS = {
    "A1_cadena_causal":        CHAIN_ELEMENTS,      # especial
    "A2_outcomes_especificos": SPECIFIC_OUTCOMES,
    "A3_diferenciacion":       STAKEHOLDER_DIFF,
    "B1_supuestos":            ASSUMPTIONS_PATTERNS,
    "B2_evidencia":            EVIDENCE_PATTERNS,
    "B3_indicadores":          INDICATORS_PATTERNS,
    "B4_temporal":             TEMPORAL_PATTERNS,
    "C1_participacion":        PARTICIPATION_PATTERNS,
    "C2_negativos":            NEGATIVE_PATTERNS,
    "C3_claridad":             CLARITY_PATTERNS,
}

DIMENSION_LABELS = {
    "A1_cadena_causal":        "A1 · Cadena causal completa",
    "A2_outcomes_especificos": "A2 · Especificidad de outcomes",
    "A3_diferenciacion":       "A3 · Diferenciación por stakeholder",
    "B1_supuestos":            "B1 · Supuestos documentados",
    "B2_evidencia":            "B2 · Base de evidencia",
    "B3_indicadores":          "B3 · Indicadores definidos",
    "B4_temporal":             "B4 · Dimensión temporal",
    "C1_participacion":        "C1 · Involucramiento de stakeholders",
    "C2_negativos":            "C2 · Outcomes negativos / riesgos",
    "C3_claridad":             "C3 · Claridad y comunicación",
}

DOMAIN_LABELS = {
    "A": "Dominio A — Arquitectura causal",
    "B": "Dominio B — Robustez epistémica",
    "C": "Dominio C — Completitud y uso",
}


def score_chain(text: str) -> float:
    """A1: Puntúa la completitud de la cadena causal (0–10)."""
    text_l = text.lower()
    found = 0
    for element, patterns in CHAIN_ELEMENTS.items():
        for p in patterns:
            if re.search(p, text_l):
                found += 1
                break
    # 5 eslabones → 10 pts. Extra pts por transiciones explícitas
    transitions = len(re.findall(
        r"\b(leads?\s+to|results?\s+in|contributes?\s+to|enables?|lleva\s+a|contribuye\s+a|permite)\b",
        text_l
    ))
    score = (found / 5) * 8 + min(transitions, 4) * 0.5
    return min(round(score, 1), 10.0)


def score_dimension(text: str, patterns: list) -> float:
    """Puntúa una dimensión basada en lista de patrones (0–10)."""
    text_l = text.lower()
    hits = 0
    total_hits = 0
    for p in patterns:
        matches = re.findall(p, text_l, flags=re.IGNORECASE)
        if matches:
            hits += 1
            total_hits += len(matches)
    # Combinar presencia (80%) + frecuencia relativa (20%)
    presence_score  = (hits / len(patterns)) * 8
    freq_score      = min(total_hits / max(len(text) / 1000, 1), 1) * 2
    return min(round(presence_score + freq_score, 1), 10.0)


def get_maturity_level(score: float) -> tuple:
    for low, high, code, name, desc in MATURITY_LEVELS:
        if low <= score <= high:
            return code, name, desc
    return "N1", "Proto-ToC", MATURITY_LEVELS[0][4]


def load_pdf_text(filename: str, max_pages: int = 30) -> str:
    """Carga el texto completo de un PDF desde pdfs/."""
    if not filename:
        return ""
    pdf_path = BASE_DIR / "pdfs" / filename
    if not pdf_path.exists():
        return ""
    try:
        from pdfminer.high_level import extract_text
        return extract_text(str(pdf_path), maxpages=max_pages) or ""
    except Exception:
        try:
            import pypdf
            reader = pypdf.PdfReader(str(pdf_path))
            pages  = min(max_pages, len(reader.pages))
            return "\n".join(reader.pages[i].extract_text() or "" for i in range(pages))
        except Exception:
            return ""


def score_document(record: dict) -> dict:
    """Calcula el IMToC para un documento extraído."""
    # Intentar cargar texto completo del PDF
    full_text = load_pdf_text(record.get("file", ""))
    text = " ".join([
        record.get("titulo", ""),
        record.get("descripcion_bd", ""),
        " ".join(record.get("snippets", {}).values()),
        full_text,
    ])

    scores = {}

    # A1 especial
    scores["A1_cadena_causal"] = score_chain(text)

    # Resto de dimensiones
    for dim, patterns in ALL_DIMENSIONS.items():
        if dim == "A1_cadena_causal":
            continue
        scores[dim] = score_dimension(text, patterns)

    # Bonus por longitud del texto (documentos más largos suelen ser más completos)
    text_chars = record.get("text_chars", 0)
    length_bonus = min(text_chars / 50000, 1.0)  # máx 1 punto extra

    # Subtotales por dominio
    domain_a = sum(scores[d] for d in ["A1_cadena_causal", "A2_outcomes_especificos", "A3_diferenciacion"])
    domain_b = sum(scores[d] for d in ["B1_supuestos", "B2_evidencia", "B3_indicadores", "B4_temporal"])
    domain_c = sum(scores[d] for d in ["C1_participacion", "C2_negativos", "C3_claridad"])

    total = domain_a + domain_b + domain_c + length_bonus
    total = min(round(total, 1), 100.0)

    code, level_name, level_desc = get_maturity_level(total)

    return {
        "id":           record.get("id", ""),
        "titulo":       record.get("titulo", "")[:80],
        "organizacion": record.get("organizacion", ""),
        "sector":       record.get("sector", ""),
        "año":          record.get("año", ""),
        "idioma":       record.get("idioma", ""),
        "nivel_toc":    record.get("nivel_toc", ""),
        **{f"score_{k}": v for k, v in scores.items()},
        "domain_A":     round(domain_a, 1),
        "domain_B":     round(domain_b, 1),
        "domain_C":     round(domain_c, 1),
        "imtoc_total":  total,
        "nivel_codigo": code,
        "nivel_nombre": level_name,
        "nivel_desc":   level_desc,
        "text_chars":   text_chars,
    }


def load_db_meta() -> dict:
    """Carga metadatos del CSV original por ID."""
    with open(CSV_DB, newline="", encoding="utf-8") as f:
        return {row["id"]: row for row in csv.DictReader(f)}


def find_pdf_for_record(row: dict) -> str:
    """Busca el PDF descargado que corresponde a un registro CSV."""
    pdf_dir = BASE_DIR / "pdfs"
    for prefix in [row['id'].zfill(3) + "_", row['id'] + "_"]:
        for f in pdf_dir.glob(f"{prefix}*.pdf"):
            return f.name
    return ""


def run():
    db_meta = load_db_meta()

    # 1. Load records from JSONL (already has snippets from web scraping)
    jsonl_ids = set()
    records = []
    if JSONL_PATH.exists():
        with open(JSONL_PATH) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    meta = db_meta.get(r["id"], {})
                    r["descripcion_bd"] = meta.get("descripcion", "")
                    r["palabras_clave"] = meta.get("palabras_clave", "")
                    if not r.get("file"):
                        r["file"] = find_pdf_for_record(meta)
                    records.append(r)
                    jsonl_ids.add(r["id"])
                except json.JSONDecodeError:
                    pass

    # 2. Add CSV records NOT in JSONL that have a downloaded PDF
    for doc_id, meta in db_meta.items():
        if doc_id in jsonl_ids:
            continue
        pdf_file = find_pdf_for_record(meta)
        if not pdf_file:
            continue
        records.append({
            "id": doc_id,
            "titulo": meta.get("titulo", ""),
            "file": pdf_file,
            "snippets": {},
            "descripcion_bd": meta.get("descripcion", ""),
            "palabras_clave": meta.get("palabras_clave", ""),
        })

    print(f"\n{'='*65}")
    print(f"  ÍNDICE DE MADUREZ DE LA TEORÍA DEL CAMBIO (IMToC)")
    print(f"  CVP EAFIT — {len(records)} documentos a evaluar")
    print(f"{'='*65}\n")

    results = []
    for r in records:
        scored = score_document(r)
        results.append(scored)

    # Ordenar por score total
    results.sort(key=lambda x: x["imtoc_total"], reverse=True)

    # ── Exportar CSV de scores ─────────────────────────────────────────────────
    fieldnames = list(results[0].keys()) if results else []
    with open(SCORES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # ── Exportar JSON para visualización ──────────────────────────────────────
    level_dist = defaultdict(int)
    for r in results:
        level_dist[r["nivel_codigo"]] += 1

    dim_avgs = {}
    for dim in ALL_DIMENSIONS:
        key = f"score_{dim}"
        vals = [r[key] for r in results if key in r]
        dim_avgs[dim] = round(sum(vals) / len(vals), 2) if vals else 0

    json_data = {
        "n_documents":    len(results),
        "avg_total":      round(sum(r["imtoc_total"] for r in results) / len(results), 1) if results else 0,
        "avg_domain_A":   round(sum(r["domain_A"] for r in results) / len(results), 1) if results else 0,
        "avg_domain_B":   round(sum(r["domain_B"] for r in results) / len(results), 1) if results else 0,
        "avg_domain_C":   round(sum(r["domain_C"] for r in results) / len(results), 1) if results else 0,
        "level_distribution": dict(sorted(level_dist.items())),
        "dimension_averages": dim_avgs,
        "documents": results,
    }
    with open(DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    # ── Generar reporte Markdown ───────────────────────────────────────────────
    avg = json_data["avg_total"]
    lines = [
        "# Índice de Madurez de la Teoría del Cambio (IMToC)",
        "## CVP EAFIT — Repositorio de ToCs",
        "",
        f"**Documentos evaluados:** {len(results)}  ",
        f"**Promedio IMToC global:** {avg}/100  ",
        f"**Fecha:** {__import__('datetime').datetime.now().strftime('%Y-%m-%d')}",
        "",
        "---",
        "",
        "## Marco de evaluación",
        "",
        "El IMToC evalúa cada Teoría del Cambio en **10 dimensiones** agrupadas en 3 dominios:",
        "",
        "| Dominio | Dimensiones | Máx |",
        "|---------|-------------|-----|",
        "| A · Arquitectura causal | Cadena causal, Especificidad outcomes, Diferenciación stakeholders | 30 |",
        "| B · Robustez epistémica | Supuestos, Evidencia, Indicadores, Temporalidad | 40 |",
        "| C · Completitud y uso | Participación, Outcomes negativos, Claridad | 30 |",
        "",
        "### Niveles de madurez",
        "",
        "| Nivel | Rango | Nombre | Descripción |",
        "|-------|-------|--------|-------------|",
    ]
    for lo, hi, code, name, desc in MATURITY_LEVELS:
        lines.append(f"| **{code}** | {lo}–{hi} | {name} | {desc} |")

    lines += [
        "",
        "---",
        "",
        "## Distribución por nivel",
        "",
        "| Nivel | Nombre | # Documentos | % |",
        "|-------|--------|:---:|:---:|",
    ]
    for lo, hi, code, name, _ in MATURITY_LEVELS:
        count = level_dist.get(code, 0)
        pct   = round(count * 100 / max(len(results), 1))
        lines.append(f"| **{code}** | {name} | {count} | {pct}% |")

    lines += [
        "",
        "---",
        "",
        "## Promedios por dimensión",
        "",
        "| Dimensión | Promedio (0–10) | Barra |",
        "|-----------|:--------------:|-------|",
    ]
    for dim, label in DIMENSION_LABELS.items():
        avg_d = dim_avgs.get(dim, 0)
        bar   = "█" * int(avg_d) + "░" * (10 - int(avg_d))
        lines.append(f"| {label} | **{avg_d}** | `{bar}` |")

    lines += [
        "",
        "---",
        "",
        "## Ranking de documentos",
        "",
        "| Pos | ID | Título | Organización | Sector | IMToC | Nivel | A | B | C |",
        "|-----|----|----------------------------------------------------|------|--------|:-----:|:-----:|:--:|:--:|:--:|",
    ]
    for i, r in enumerate(results, 1):
        title = r["titulo"][:45]
        lines.append(
            f"| {i} | {r['id']} | {title} | {r['organizacion'][:25]} "
            f"| {r['sector'][:18]} | **{r['imtoc_total']}** | {r['nivel_codigo']} {r['nivel_nombre']} "
            f"| {r['domain_A']} | {r['domain_B']} | {r['domain_C']} |"
        )

    lines += [
        "",
        "---",
        "",
        "## Análisis por dominio",
        "",
        f"- **Dominio A (Arquitectura causal):** promedio {json_data['avg_domain_A']}/30",
        f"- **Dominio B (Robustez epistémica):** promedio {json_data['avg_domain_B']}/40",
        f"- **Dominio C (Completitud y uso):** promedio {json_data['avg_domain_C']}/30",
        "",
        "### Dimensión más débil en el corpus",
        "",
    ]
    weakest = min(dim_avgs, key=dim_avgs.get)
    strongest = max(dim_avgs, key=dim_avgs.get)
    lines += [
        f"- **Más débil:** {DIMENSION_LABELS[weakest]} — promedio **{dim_avgs[weakest]}/10**",
        f"- **Más fuerte:** {DIMENSION_LABELS[strongest]} — promedio **{dim_avgs[strongest]}/10**",
        "",
        "---",
        "",
        "*IMToC v1.0 — CVP EAFIT 2026. Scoring automatizado sobre texto extraído de PDFs.*",
        "*El índice es orientativo; la evaluación experta sigue siendo el estándar de referencia.*",
    ]

    with open(REPORT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # ── Imprimir en consola ────────────────────────────────────────────────────
    print(f"{'Pos':>3} {'ID':>3}  {'Título':<45}  {'IMToC':>5}  {'Nivel':<22}  A/{'' :>2}B/{'' :>2}C")
    print("-" * 100)
    for i, r in enumerate(results, 1):
        print(
            f"{i:>3} [{r['id']:>2}]  {r['titulo'][:45]:<45}  {r['imtoc_total']:>5}  "
            f"{r['nivel_codigo']} {r['nivel_nombre']:<18}  "
            f"{r['domain_A']:>4}/{r['domain_B']:>4}/{r['domain_C']:>4}"
        )

    print(f"\n{'='*65}")
    print(f"  RESUMEN GLOBAL")
    print(f"  Promedio IMToC    : {json_data['avg_total']}/100")
    print(f"  Dominio A (causal): {json_data['avg_domain_A']}/30")
    print(f"  Dominio B (epist.): {json_data['avg_domain_B']}/40")
    print(f"  Dominio C (compl.): {json_data['avg_domain_C']}/30")
    print(f"\n  Distribución por nivel:")
    for lo, hi, code, name, _ in MATURITY_LEVELS:
        count = level_dist.get(code, 0)
        bar   = "▓" * count
        print(f"  {code} {name:<22} {bar} {count}")
    print(f"\n  Dimensión más débil  : {DIMENSION_LABELS[weakest]} ({dim_avgs[weakest]}/10)")
    print(f"  Dimensión más fuerte : {DIMENSION_LABELS[strongest]} ({dim_avgs[strongest]}/10)")
    print(f"\n  Archivos generados:")
    print(f"  → {SCORES_CSV}")
    print(f"  → {REPORT_MD}")
    print(f"  → {DATA_JSON}")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    run()
