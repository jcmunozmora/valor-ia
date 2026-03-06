"""
SROI Meta-Analysis Extractor
Usa Claude API (Haiku) para extraer información estructurada de cada reporte.
Costo estimado: ~$0.40 para los 383 reportes completos.
Genera una base de datos rica para meta-análisis y artículo académico.
"""

import sqlite3
import json
import time
import os
import re
from pathlib import Path
import openai

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "sroi_reports.db"

# Cliente Anthropic (inicializado en main() después de verificar la API key)
client = None

# Modelo: gpt-4o-mini para velocidad y costo; cambiar a gpt-4o para mayor precisión
MODEL = "gpt-4o-mini"
DELAY = 0.5  # segundos entre llamadas API

EXTRACTION_PROMPT = """Eres un experto en análisis de reportes SROI (Social Return on Investment). Extrae la información estructurada del siguiente texto de un reporte SROI y devuelve ÚNICAMENTE un JSON válido con los campos indicados.

Si un campo no se menciona explícitamente en el texto, usa null. No inventes datos.

TEXTO DEL REPORTE:
{text}

TÍTULO DEL REPORTE: {title}

Devuelve SOLO el siguiente JSON (sin markdown, sin explicaciones):

{{
  "programa": {{
    "nombre": "nombre específico del programa evaluado (no el nombre de la organización)",
    "organizacion": "nombre de la organización que implementa el programa",
    "pais": "país o región principal (ej: UK, Australia, Canada, USA, Colombia)",
    "ciudad_region": "ciudad o región específica si se menciona",
    "anio_publicacion": número_entero_o_null,
    "anio_intervencion": "año o rango de años del período evaluado (ej: 2010-2011)",
    "duracion_meses": número_entero_o_null,
    "sector": "uno de: [empleo, salud, educacion, vivienda, medio_ambiente, inclusion_social, agricultura, infancia_juventud, personas_mayores, discapacidad, justicia, microfinanzas, arte_cultura, deporte, otro]",
    "poblacion_objetivo": "descripción breve de los beneficiarios principales",
    "tamano_muestra": número_entero_o_null,
    "tipo_reporte": "uno de: [evaluativo, prospectivo, mixto]"
  }},
  "sroi": {{
    "ratio": número_decimal_o_null,
    "ratio_min": número_decimal_o_null,
    "ratio_max": número_decimal_o_null,
    "inversion_total": "monto en moneda original con símbolo (ej: £45,000)",
    "valor_social_total": "monto total de valor social generado con símbolo",
    "moneda": "GBP, USD, AUD, EUR, CAD, u otra",
    "periodo_analisis_anios": número_decimal_o_null,
    "tasa_descuento_pct": número_decimal_o_null
  }},
  "metodologia": {{
    "tipo_analisis": "uno de: [SROI_puro, costo_beneficio, analisis_impacto_social, otro]",
    "asegurado_svi": true_o_false,
    "stakeholders": {{
      "n_grupos": número_entero_o_null,
      "grupos_principales": ["lista de grupos de stakeholders mencionados"],
      "metodo_involucramiento": ["encuesta", "entrevista", "taller", "grupo_focal", "consulta_secundaria", "datos_admin"]
    }},
    "recoleccion_datos": ["lista de métodos usados: encuesta, entrevista, registros_admin, bases_datos_secundarias, revisión_literatura, grupo_focal, observacion"],
    "n_outcomes_total": número_entero_o_null,
    "n_outcomes_monetizados": número_entero_o_null,
    "fuentes_proxy": ["lista de fuentes para proxies: HACT, WELLBY, NHS_costs, DWP, ONS, WTP_survey, costo_evitado, salario_referencia, otro"],
    "analisis_sensibilidad": true_o_false,
    "materialidad_aplicada": true_o_false
  }},
  "factores_impacto": {{
    "peso_muerto_pct": número_decimal_o_null,
    "peso_muerto_metodo": "cómo se estimó: encuesta_beneficiarios, benchmark_literatura, grupo_control, datos_mercado, expertos, asumido",
    "atribucion_pct": número_decimal_o_null,
    "atribucion_metodo": "cómo se estimó: encuesta_beneficiarios, benchmark, asumido, otro",
    "desplazamiento_pct": número_decimal_o_null,
    "drop_off_pct_anual": número_decimal_o_null,
    "duracion_outcomes_anios": número_decimal_o_null
  }},
  "outcomes_principales": [
    {{
      "stakeholder": "grupo que lo experimenta",
      "descripcion": "descripción del outcome en 1-2 frases",
      "tipo": "uno de: [economico, salud, bienestar, social, ambiental, educativo]",
      "proxy_usado": "indicador financiero utilizado para monetizar",
      "valor_unitario": "valor monetario por persona/unidad si se menciona"
    }}
  ],
  "calidad_metodologica": {{
    "limitaciones_reconocidas": true_o_false,
    "describe_contrafactual": true_o_false,
    "transparencia_supuestos": "alta, media, baja",
    "rigor_general": "alto, medio, bajo",
    "notas_calidad": "observación breve sobre la calidad metodológica del reporte"
  }},
  "meta": {{
    "texto_suficiente": true_o_false,
    "confianza_extraccion": "alta, media, baja",
    "notas": "observaciones sobre limitaciones de la extracción"
  }}
}}"""


def create_meta_schema(conn):
    """Crea las columnas de meta-análisis en la tabla de reportes."""
    new_columns = [
        # Programa
        ("meta_programa_nombre", "TEXT"),
        ("meta_organizacion", "TEXT"),
        ("meta_pais", "TEXT"),
        ("meta_ciudad_region", "TEXT"),
        ("meta_anio_publicacion", "INTEGER"),
        ("meta_anio_intervencion", "TEXT"),
        ("meta_duracion_meses", "INTEGER"),
        ("meta_sector", "TEXT"),
        ("meta_poblacion_objetivo", "TEXT"),
        ("meta_tamano_muestra", "INTEGER"),
        ("meta_tipo_reporte", "TEXT"),
        # SROI
        ("meta_sroi_ratio", "REAL"),
        ("meta_sroi_ratio_min", "REAL"),
        ("meta_sroi_ratio_max", "REAL"),
        ("meta_inversion_total", "TEXT"),
        ("meta_valor_social_total", "TEXT"),
        ("meta_moneda", "TEXT"),
        ("meta_periodo_anios", "REAL"),
        ("meta_tasa_descuento_pct", "REAL"),
        # Metodología
        ("meta_tipo_analisis", "TEXT"),
        ("meta_asegurado_svi", "INTEGER"),
        ("meta_n_stakeholder_grupos", "INTEGER"),
        ("meta_grupos_stakeholders", "TEXT"),  # JSON array
        ("meta_metodo_involucramiento", "TEXT"),  # JSON array
        ("meta_recoleccion_datos", "TEXT"),  # JSON array
        ("meta_n_outcomes_total", "INTEGER"),
        ("meta_n_outcomes_monetizados", "INTEGER"),
        ("meta_fuentes_proxy", "TEXT"),  # JSON array
        ("meta_analisis_sensibilidad", "INTEGER"),
        ("meta_materialidad", "INTEGER"),
        # Factores de impacto
        ("meta_peso_muerto_pct", "REAL"),
        ("meta_peso_muerto_metodo", "TEXT"),
        ("meta_atribucion_pct", "REAL"),
        ("meta_atribucion_metodo", "TEXT"),
        ("meta_desplazamiento_pct", "REAL"),
        ("meta_drop_off_pct", "REAL"),
        ("meta_duracion_outcomes_anios", "REAL"),
        # Outcomes principales
        ("meta_outcomes_json", "TEXT"),  # JSON completo
        # Calidad
        ("meta_limitaciones_reconocidas", "INTEGER"),
        ("meta_describe_contrafactual", "INTEGER"),
        ("meta_transparencia_supuestos", "TEXT"),
        ("meta_rigor_general", "TEXT"),
        ("meta_notas_calidad", "TEXT"),
        # Control
        ("meta_extracted", "INTEGER DEFAULT 0"),
        ("meta_confianza", "TEXT"),
        ("meta_notas_extraccion", "TEXT"),
        ("meta_extracted_at", "TEXT"),
        ("meta_raw_json", "TEXT"),  # JSON completo para auditoría
    ]

    for col_name, col_type in new_columns:
        try:
            conn.execute(f"ALTER TABLE reports ADD COLUMN {col_name} {col_type}")
        except sqlite3.OperationalError:
            pass  # Columna ya existe
    conn.commit()
    print(f"Schema updated: {len(new_columns)} columns ready")


def extract_with_openai(title, pdf_text, abstract=None):
    """Llama a OpenAI API para extraer metadatos estructurados."""
    # Combinar texto disponible
    text = pdf_text or ""
    if abstract and len(abstract) > 100:
        text = f"ABSTRACT: {abstract}\n\n---\n\nTEXTO PDF:\n{text}"

    text = text[:5000]  # Límite de contexto

    prompt = EXTRACTION_PROMPT.format(text=text, title=title or "Desconocido")

    response = client.chat.completions.create(
        model=MODEL,
        max_tokens=2000,
        response_format={"type": "json_object"},
        messages=[{"role": "user", "content": prompt}]
    )

    raw_text = response.choices[0].message.content.strip()

    # Limpiar markdown si está presente
    raw_text = re.sub(r'^```json\s*', '', raw_text)
    raw_text = re.sub(r'\s*```$', '', raw_text)

    return json.loads(raw_text)


def flatten_extracted(data):
    """Convierte el JSON extraído a campos planos para SQLite."""
    p = data.get("programa", {})
    s = data.get("sroi", {})
    m = data.get("metodologia", {})
    st = m.get("stakeholders", {})
    f = data.get("factores_impacto", {})
    c = data.get("calidad_metodologica", {})
    meta = data.get("meta", {})

    return {
        # Programa
        "meta_programa_nombre": p.get("nombre"),
        "meta_organizacion": p.get("organizacion"),
        "meta_pais": p.get("pais"),
        "meta_ciudad_region": p.get("ciudad_region"),
        "meta_anio_publicacion": p.get("anio_publicacion"),
        "meta_anio_intervencion": p.get("anio_intervencion"),
        "meta_duracion_meses": p.get("duracion_meses"),
        "meta_sector": p.get("sector"),
        "meta_poblacion_objetivo": p.get("poblacion_objetivo"),
        "meta_tamano_muestra": p.get("tamano_muestra"),
        "meta_tipo_reporte": p.get("tipo_reporte"),
        # SROI
        "meta_sroi_ratio": s.get("ratio"),
        "meta_sroi_ratio_min": s.get("ratio_min"),
        "meta_sroi_ratio_max": s.get("ratio_max"),
        "meta_inversion_total": s.get("inversion_total"),
        "meta_valor_social_total": s.get("valor_social_total"),
        "meta_moneda": s.get("moneda"),
        "meta_periodo_anios": s.get("periodo_analisis_anios"),
        "meta_tasa_descuento_pct": s.get("tasa_descuento_pct"),
        # Metodología
        "meta_tipo_analisis": m.get("tipo_analisis"),
        "meta_asegurado_svi": 1 if m.get("asegurado_svi") else 0,
        "meta_n_stakeholder_grupos": st.get("n_grupos"),
        "meta_grupos_stakeholders": json.dumps(st.get("grupos_principales", []), ensure_ascii=False),
        "meta_metodo_involucramiento": json.dumps(st.get("metodo_involucramiento", []), ensure_ascii=False),
        "meta_recoleccion_datos": json.dumps(m.get("recoleccion_datos", []), ensure_ascii=False),
        "meta_n_outcomes_total": m.get("n_outcomes_total"),
        "meta_n_outcomes_monetizados": m.get("n_outcomes_monetizados"),
        "meta_fuentes_proxy": json.dumps(m.get("fuentes_proxy", []), ensure_ascii=False),
        "meta_analisis_sensibilidad": 1 if m.get("analisis_sensibilidad") else 0,
        "meta_materialidad": 1 if m.get("materialidad_aplicada") else 0,
        # Factores de impacto
        "meta_peso_muerto_pct": f.get("peso_muerto_pct"),
        "meta_peso_muerto_metodo": f.get("peso_muerto_metodo"),
        "meta_atribucion_pct": f.get("atribucion_pct"),
        "meta_atribucion_metodo": f.get("atribucion_metodo"),
        "meta_desplazamiento_pct": f.get("desplazamiento_pct"),
        "meta_drop_off_pct": f.get("drop_off_pct_anual"),
        "meta_duracion_outcomes_anios": f.get("duracion_outcomes_anios"),
        # Outcomes (JSON completo)
        "meta_outcomes_json": json.dumps(data.get("outcomes_principales", []), ensure_ascii=False),
        # Calidad
        "meta_limitaciones_reconocidas": 1 if c.get("limitaciones_reconocidas") else 0,
        "meta_describe_contrafactual": 1 if c.get("describe_contrafactual") else 0,
        "meta_transparencia_supuestos": c.get("transparencia_supuestos"),
        "meta_rigor_general": c.get("rigor_general"),
        "meta_notas_calidad": c.get("notas_calidad"),
        # Control
        "meta_confianza": meta.get("confianza_extraccion"),
        "meta_notas_extraccion": meta.get("notas"),
    }


def build_update_sql(fields):
    """Construye el SQL de UPDATE dinámicamente."""
    set_clause = ", ".join(f"{k} = :{k}" for k in fields)
    return f"UPDATE reports SET {set_clause}, meta_extracted = 1, meta_extracted_at = datetime('now') WHERE id = :id"


def export_meta_csv(conn, output_path):
    """Exporta la tabla meta-análisis a CSV para análisis estadístico."""
    import csv
    cols = [
        "id", "slug", "title", "pdf_url",
        "meta_programa_nombre", "meta_organizacion", "meta_pais", "meta_ciudad_region",
        "meta_anio_publicacion", "meta_anio_intervencion", "meta_duracion_meses",
        "meta_sector", "meta_poblacion_objetivo", "meta_tamano_muestra", "meta_tipo_reporte",
        "meta_sroi_ratio", "meta_sroi_ratio_min", "meta_sroi_ratio_max",
        "meta_inversion_total", "meta_valor_social_total", "meta_moneda",
        "meta_periodo_anios", "meta_tasa_descuento_pct",
        "meta_tipo_analisis", "meta_asegurado_svi",
        "meta_n_stakeholder_grupos", "meta_grupos_stakeholders",
        "meta_metodo_involucramiento", "meta_recoleccion_datos",
        "meta_n_outcomes_total", "meta_n_outcomes_monetizados", "meta_fuentes_proxy",
        "meta_analisis_sensibilidad", "meta_materialidad",
        "meta_peso_muerto_pct", "meta_peso_muerto_metodo",
        "meta_atribucion_pct", "meta_atribucion_metodo",
        "meta_desplazamiento_pct", "meta_drop_off_pct", "meta_duracion_outcomes_anios",
        "meta_limitaciones_reconocidas", "meta_describe_contrafactual",
        "meta_transparencia_supuestos", "meta_rigor_general", "meta_notas_calidad",
        "meta_confianza",
    ]

    rows = conn.execute(f"SELECT {', '.join(cols)} FROM reports WHERE meta_extracted=1").fetchall()
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"CSV exportado: {output_path} ({len(rows)} filas)")


def print_stats(conn):
    """Muestra estadísticas del meta-análisis."""
    total = conn.execute("SELECT COUNT(*) FROM reports WHERE meta_extracted=1").fetchone()[0]
    print(f"\n{'='*55}")
    print(f"META-ANÁLISIS SROI — {total} REPORTES PROCESADOS")
    print(f"{'='*55}")

    # Por sector
    print("\nPor sector:")
    for row in conn.execute("""
        SELECT meta_sector, COUNT(*) as n, ROUND(AVG(meta_sroi_ratio),2) as avg_ratio
        FROM reports WHERE meta_extracted=1 AND meta_sector IS NOT NULL
        GROUP BY meta_sector ORDER BY n DESC
    """):
        print(f"  {row[0]:<25} n={row[1]:3d}  ratio_promedio={row[2] or 'N/A'}")

    # Por país
    print("\nPor país (top 10):")
    for row in conn.execute("""
        SELECT meta_pais, COUNT(*) as n FROM reports
        WHERE meta_extracted=1 AND meta_pais IS NOT NULL
        GROUP BY meta_pais ORDER BY n DESC LIMIT 10
    """):
        print(f"  {row[0]:<20} n={row[1]}")

    # Factores de impacto
    print("\nFactores de impacto (promedios):")
    row = conn.execute("""
        SELECT
            ROUND(AVG(meta_peso_muerto_pct),1) as dw,
            ROUND(AVG(meta_atribucion_pct),1) as att,
            ROUND(AVG(meta_desplazamiento_pct),1) as disp,
            ROUND(AVG(meta_drop_off_pct),1) as drop,
            COUNT(CASE WHEN meta_peso_muerto_pct IS NOT NULL THEN 1 END) as n_dw
        FROM reports WHERE meta_extracted=1
    """).fetchone()
    print(f"  Peso muerto: {row[0]}% (n={row[4]}) | Atribución: {row[1]}% | Desplazamiento: {row[2]}% | Drop-off: {row[3]}%")

    # Calidad
    sa = conn.execute("SELECT COUNT(*) FROM reports WHERE meta_extracted=1 AND meta_analisis_sensibilidad=1").fetchone()[0]
    lim = conn.execute("SELECT COUNT(*) FROM reports WHERE meta_extracted=1 AND meta_limitaciones_reconocidas=1").fetchone()[0]
    print(f"\nCalidad: Sensibilidad={sa}/{total} | Limitaciones reconocidas={lim}/{total}")


def main():
    global client

    # Verificar API key
    if not os.environ.get("OPENAI_API_KEY"):
        print("ERROR: OPENAI_API_KEY no está configurada")
        print("Configura con: export OPENAI_API_KEY='tu-clave'")
        return

    client = openai.OpenAI()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Crear schema
    create_meta_schema(conn)

    # Reportes pendientes (con texto PDF pero sin meta-análisis)
    reports = conn.execute("""
        SELECT id, slug, title, pdf_text_extract, abstract
        FROM reports
        WHERE (meta_extracted IS NULL OR meta_extracted = 0)
        AND pdf_text_extract IS NOT NULL
        AND length(pdf_text_extract) > 500
        ORDER BY sroi_ratio_pdf DESC NULLS LAST, id
    """).fetchall()

    total_pending = len(reports)
    print(f"Reportes a procesar: {total_pending}")
    print(f"Modelo: {MODEL}")
    print(f"Costo estimado: ~${total_pending * 0.001:.2f} USD\n")

    processed = 0
    errors = 0

    for i, report in enumerate(reports):
        report_id = report["id"]
        slug = report["slug"]
        title = report["title"] or slug

        print(f"[{i+1}/{total_pending}] {slug[:55]}", end="", flush=True)

        try:
            extracted = extract_with_openai(
                title=title,
                pdf_text=report["pdf_text_extract"],
                abstract=report["abstract"]
            )

            # Guardar JSON completo para auditoría
            raw_json = json.dumps(extracted, ensure_ascii=False)

            # Aplanar para SQLite
            flat = flatten_extracted(extracted)
            flat["id"] = report_id
            flat["meta_raw_json"] = raw_json

            # UPDATE
            sql = build_update_sql({k: v for k, v in flat.items() if k != "id"})
            conn.execute(sql, flat)
            conn.commit()

            ratio = extracted.get("sroi", {}).get("ratio")
            sector = extracted.get("programa", {}).get("sector", "?")
            pais = extracted.get("programa", {}).get("pais", "?")
            conf = extracted.get("meta", {}).get("confianza_extraccion", "?")
            print(f" → ratio={ratio} | sector={sector} | país={pais} | conf={conf}")
            processed += 1

        except json.JSONDecodeError as e:
            print(f" → ERROR JSON: {e}")
            conn.execute("UPDATE reports SET meta_extracted=9 WHERE id=?", (report_id,))
            conn.commit()
            errors += 1

        except Exception as e:
            print(f" → ERROR: {e}")
            conn.execute("UPDATE reports SET meta_extracted=9 WHERE id=?", (report_id,))
            conn.commit()
            errors += 1

        time.sleep(DELAY)

    print(f"\nCompleto: {processed} procesados | {errors} errores")

    # Exportar
    print_stats(conn)

    csv_path = DB_PATH.parent / "sroi_meta_analysis.csv"
    export_meta_csv(conn, csv_path)

    # JSONL enriquecido
    jsonl_path = DB_PATH.parent / "sroi_meta_analysis.jsonl"
    rows = conn.execute("""
        SELECT * FROM reports WHERE meta_extracted=1 ORDER BY meta_sroi_ratio DESC NULLS LAST
    """).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM reports LIMIT 0").description]
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for row in rows:
            record = {k: v for k, v in zip(cols, row) if v is not None}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"JSONL enriquecido: {jsonl_path}")

    conn.close()


if __name__ == "__main__":
    main()
