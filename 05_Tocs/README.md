# Repositorio de Teorías del Cambio — CVP EAFIT

Banco de Teorías del Cambio (ToC) de organizaciones internacionales, fundaciones, gobiernos, ONGs y organismos multilaterales. Cubre todos los sectores relevantes para el análisis de impacto social.

---

## Estructura del repositorio

```
05_Tocs/
├── pdfs/                    ← PDFs descargados (gitignore en repo grande)
├── images/                  ← Diagramas de ToC extraídos de PDFs
├── metadata/
│   ├── toc_database.csv     ← Base de datos maestra (ID, título, org, sector, URL, estado)
│   ├── toc_extracted.jsonl  ← Texto y elementos detectados por PDF (generado por script 02)
│   ├── download_checkpoint.json ← Estado de cada descarga
│   ├── summary_report.md   ← Reporte estadístico del repositorio
│   └── stats.json          ← Estadísticas en formato JSON
└── scripts/
    ├── 01_download_tocs.py  ← Descarga PDFs desde toc_database.csv
    ├── 02_extract_toc_text.py ← Extrae texto y detecta elementos de ToC
    ├── 03_search_more_tocs.py ← Búsqueda automatizada de nuevas ToCs
    └── 04_build_summary.py  ← Genera reporte resumen del repositorio
```

---

## Pipeline de trabajo

### Paso 1 — Descargar los PDFs curados

```bash
cd 05_Tocs/scripts
pip install requests pandas
python 01_download_tocs.py
```

Descarga los 60 registros iniciales. Usa checkpoint — se puede interrumpir y reanudar.

### Paso 2 — Extraer texto y elementos de ToC

```bash
pip install pdfminer.six
python 02_extract_toc_text.py
```

Genera `metadata/toc_extracted.jsonl` con texto, elementos detectados (inputs, outcomes, supuestos, etc.) y snippets clave.

### Paso 3 — Buscar más ToCs automáticamente

```bash
pip install beautifulsoup4
python 03_search_more_tocs.py
```

Ejecuta ~25 búsquedas predefinidas por sector y agrega nuevos registros al CSV. Luego correr `01_download_tocs.py` nuevamente para descargar los nuevos.

### Paso 4 — Generar reporte de estado

```bash
python 04_build_summary.py
```

Genera `metadata/summary_report.md` con estadísticas por sector, idioma, tipo de organización y elementos de ToC detectados.

---

## Base de datos inicial (60 registros curados)

| Sector | Registros |
|--------|-----------|
| Metodología / Guías | 13 |
| Desarrollo Internacional / ONU | 7 |
| Salud mental / Bienestar | 6 |
| Nutrición / Seguridad alimentaria | 5 |
| SROI / Valoración social | 5 |
| Empleo / Juventud | 5 |
| América Latina (español) | 5 |
| Vivienda / Clima | 4 |
| Educación | 3 |
| Gobernanza / Filantropía | 7 |

**Idiomas**: 44 EN · 16 ES
**Años**: 2004–2025
**Tipos de organización**: ONU, Fundación, ONG Internacional, Think tank, Universidad, Gobierno, Consultoría

---

## Campos del CSV

| Campo | Descripción |
|-------|-------------|
| `id` | Identificador único |
| `titulo` | Título completo del documento |
| `organizacion` | Nombre de la organización autora |
| `tipo_org` | Tipo (ONU, Fundación, ONG, Universidad, Gobierno, etc.) |
| `sector` | Sector principal (Salud, Educación, Empleo, etc.) |
| `subsector` | Subsector o tema específico |
| `pais_contexto` | País o región de aplicación |
| `año` | Año de publicación |
| `idioma` | EN o ES (principalmente) |
| `tipo_documento` | Guía, Paper académico, Documento estratégico, etc. |
| `nivel_toc` | Marco general / Programa específico / Institucional / Portafolio |
| `url` | URL de la página del documento |
| `url_pdf` | URL directa al PDF (si disponible) |
| `descripcion` | Resumen del contenido y elementos de ToC |
| `palabras_clave` | Keywords para búsqueda |
| `estado_descarga` | pendiente / ok / error / sin_url |

---

## Uso del repositorio para análisis SROI

Cuando se construye un **Impact Map / ToC** para un análisis SROI, buscar en este repositorio:

1. **Por sector** — ToCs de organizaciones similares para validar cadenas causales
2. **Por tipo de outcome** — Supuestos y factores que otros han documentado
3. **Por contexto geográfico** — ToCs de programas similares en el mismo país o región
4. **Por nivel** — Comparar ToC de programa específico vs. institucional

```python
# Ejemplo de búsqueda en el CSV
import pandas as pd
df = pd.read_csv('metadata/toc_database.csv')
salud = df[df['sector'].str.contains('Salud', case=False)]
print(salud[['titulo', 'organizacion', 'año']].to_string())
```

---

## Dependencias Python

```
requests>=2.28
beautifulsoup4>=4.11
pdfminer.six>=20221105
pandas>=1.5
```

---

Centro Valor Público · Universidad EAFIT · 2026
