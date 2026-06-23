# From Heterogeneous Bibliographic Data to a Unified Schema
## A Python ETL for Bibliometrix-like Analyses — BASE LEVEL

This report documents a source-agnostic **Extract → Transform → Load** pipeline
added to *bibliometrix-python*. It plays the same role as the `convert2df()`
function of the R version of *bibliometrix*: it turns a raw file manually
exported from any supported bibliographic database (Scopus, Dimensions, PubMed,
Lens, Web of Science, Cochrane) into a single standardized DataFrame that the
existing analytical functions can consume without crashing.

The guiding principle of this contribution was **minimal, surgical change**:
the heavy per-source parsing that already worked is reused as-is; only the
missing "spine" (a single entry point, type enforcement, null handling, schema
guarantee and validation) was added.

---

## 1. Problems identified in the current Python implementation

| # | Problem (assignment §2) | Where it shows up | How the ETL fixes it |
|---|--------------------------|-------------------|----------------------|
| 1 | No single entry point like `convert2df()` | loading logic spread over `get_data.py`, `biblio_json`, `process_single_file` | one public function `convert2df(filepath, source)` |
| 2 | Scattered / non-centralized transformation logic | per-column `format_*` functions called from one big dict literal | a centralized `FORMATTERS` mapping dictionary + 3 named phase functions |
| 3 | Weak / inconsistent type enforcement | `PY`, `TC` produced as **strings** (e.g. `str(entry['Year'])`); only saved by the `pd.read_json` round-trip, which silently fails when a column is mixed | explicit `TYPE_CONTRACTS`: `PY`/`TC` → `int`, multi-value → `list[str]` |
| 4 | Poor handling of missing values | `str(entry['References'])` on a missing cell yields the literal `"nan"`; `None` cells leak into functions | null handling: scalars → `""`, lists → `[]`, `TC`/`PY` → `0` |
| 5 | Implicit dependency on Web of Science | functions assume WoS column shapes | a *dispatcher* maps every source to the same target schema and `DB` label |
| 6 | Incomplete column mapping | optional columns silently absent | the 24 mandatory columns are always created (empty if the source lacks them) |
| 7 | Non-standard parsing of references / SR | SR computed ad hoc | SR is delegated to the existing `metaTagExtraction(df, "SR")` service |

---

## 2. Architecture

The pipeline lives in a single new module, `www/services/standardizer.py`, and
follows the three mandatory sequential phases. A monolithic function was
explicitly avoided.

```
                 convert2df(filepath, source)        <-- single entry point
                          │
        ┌─────────────────┼──────────────────────────┐
        ▼                 ▼                           ▼
   EXTRACT            TRANSFORM                    LOAD
   extract()          transform()                 add_calculated_fields()
   pandas / parsers   FORMATTERS + TYPE_CONTRACTS  metaTagExtraction("SR")
                                                   validate()
```

### 2.1 The Dispatcher (EXTRACT)

`extract(filepath, source)` selects the right reader from the source id and the
file extension:

* tabular sources → `pandas.read_csv` / `pandas.read_excel`
  (Dimensions uses `skiprows=1` to skip its export banner);
* text sources → the rudimentary parsers already present in
  `www/services/parsers.py` (`parse_wos_data`, `parse_pubmed_data`,
  `parse_cochrane_data`).

Two small dictionaries drive the dispatcher and remove the implicit WoS bias:

```python
SOURCE_ALIASES = {"wos": "Web_of_Science", "scopus": "Scopus",
                  "dimensions": "Dimensions", "lens": "The_Lens",
                  "pubmed": "PubMed", "cochrane": "Cochrane"}

DB_LABELS = {"wos": "WEB_OF_SCIENCE", "scopus": "SCOPUS", ...}
```

### 2.2 The Mapping dictionary (TRANSFORM — RENAME)

Instead of scattering the proprietary→WoS mapping across the code, a single
**lookup table** associates each target WoS field tag with the function able to
extract it for *any* source:

```python
FORMATTERS = {
    "AU": format_au_column,   "AF": format_af_column,
    "C1": format_c1_column,   "CR": format_cr_column,
    "DE": format_de_column,   "ID": format_id_column,
    "PY": format_py_column,   "TC": format_tc_column,
    "SO": format_so_column,   "JI": format_ji_column,
    ...                                            # 23 entries
}
```

`transform()` loops over this dictionary once per record. The per-source
parsing itself is **reused** from the existing `format_functions.py`: those
functions are already correct and already handle Scopus/Dimensions/Lens/PubMed,
so re-implementing them would add risk for no benefit (assignment principle:
"utilize the rudimentary parsers already present").

### 2.3 The Type Contracts (TRANSFORM — TYPING & NULLS)

Type errors and unhandled nulls were the primary cause of crashes. The contract
is declared once and enforced uniformly:

```python
TYPE_CONTRACTS = {
    # scalars  -> str, null -> ""
    "DB": str, "UT": str, "DI": str, "PMID": str, "TI": str, "SO": str,
    "JI": str, "DT": str, "LA": str, "RP": str, "AB": str, "VL": str,
    "IS": str, "BP": str, "EP": str, "SR": str,
    # numeric  -> int, null -> 0
    "PY": int, "TC": int,
    # multi-value -> list[str], null -> []
    "AU": list, "AF": list, "C1": list, "CR": list, "DE": list, "ID": list,
}
```

The cleaners also remove the literal `"nan"`/`"none"` strings that pandas
produces from missing cells, and split a flat semicolon-delimited string back
into a list when needed (the `;` internal delimiter standard).

### 2.4 Calculated field SR (LOAD)

As required, SR is **not** re-implemented. `add_calculated_fields()` wraps the
DataFrame in a tiny `_DataHolder` (exposing `.get()`/`.set()`) and calls the
existing `metaTagExtraction(df, "SR")` service, which produces the canonical
`SR` (with cross-corpus disambiguation) and `SR_FULL` columns used by the
citation-network analyses.

### 2.5 Validation (LOAD)

`validate(df)` programmatically verifies the output contract and returns a
report `{"valid", "errors", "n_rows"}`:

1. all 24 mandatory columns exist;
2. no `NaN`/`None` remains;
3. multi-value columns are `list`;
4. `PY`/`TC` are integers.

---

## 3. Standardized target schema (assignment §4.2)

`convert2df` always returns the 24 mandatory columns below (plus the helpers
`AU_UN` and `SR_FULL`). Missing source data yields an empty, correctly-typed
value, never a missing column.

`DB, UT, DI, PMID, TI, SO, JI, PY, DT, LA, TC, AU, AF, C1, RP, CR, DE, ID, AB,
VL, IS, BP, EP, SR`

---

## 4. Validation against the analytical functions

A representative set of analytical functions from `functions/` was run on the
standardized DataFrame of each source. **40 / 40 executions passed** (see
`EXECUTION_LOG.md`):

```
function                         scopus dimensions       lens     pubmed        wos
get_annual_production              PASS       PASS       PASS       PASS       PASS
get_average_citations              PASS       PASS       PASS       PASS       PASS
get_relevant_sources               PASS       PASS       PASS       PASS       PASS
get_relevant_authors               PASS       PASS       PASS       PASS       PASS
get_sources_production             PASS       PASS       PASS       PASS       PASS
get_main_informations              PASS       PASS       PASS       PASS       PASS
get_lotka_law                      PASS       PASS       PASS       PASS       PASS
get_bradford_law                   PASS       PASS       PASS       PASS       PASS
```

These functions were chosen because together they exercise every critical part
of the schema: numeric years (`get_annual_production`, `get_sources_production`
which does `PY.astype(str).astype(int)`), numeric citations
(`get_average_citations`), list-valued authors (`get_relevant_authors`), the
journal field (`get_relevant_sources`, `bradford`), and the heaviest consumer
`get_main_informations`, which iterates `AU`, `DE`, `CR` as lists and derives
countries from `C1` through `metaTagExtraction("AU_CO")`.

### Debugging / patches applied to analytical functions

**None were required.** Because the data is standardized correctly (right
column names, `list` types, integer `PY`/`TC`, no `NaN`), the functions that
were "WoS-only" run unchanged on the other sources. This is the intended
outcome of the assignment: a robust ETL removes the need to patch downstream
logic. (Had a function still failed on hardcoded WoS logic, the contract of the
assignment would have been to patch that specific function; that case did not
arise for the tested set.)

One provenance detail worth noting: the `DB` label is set to the upper-case
values from the glossary (`SCOPUS`, `WEB_OF_SCIENCE`, ...). This matches the
checks already present in the services (e.g. `metatagextraction.SR` tests
`DB == "scopus"`, `biblionetwork` tests `DB == "SCOPUS"`), so SR and reference
handling behave correctly per source.

---

## 5. Files changed

The change set is deliberately small.

| File | Change | Why |
|------|--------|-----|
| `www/services/standardizer.py` | **new module** | the entire ETL pipeline (dispatcher, mapping dict, type contracts, SR, validation, `convert2df`, `standardized_to_csv`) |
| `www/services/__init__.py` | **+1 line** (`from .standardizer import *`) | expose `convert2df` to the rest of the app |
| `functions/get_data.py` | single-file load now calls `convert2df` first, with a fallback to the original `biblio_json` path | make the dashboard use the robust pipeline for uploaded files, without breaking `.bib`/zip/multi-file loading |
| `etl_demo.py` | **new script** | execution evidence: standardizes every shipped dataset and writes flat CSVs to `sources/standardized/` |
| `EXECUTION_LOG.md` | **new** | the compatibility matrix and validation results |
| `ETL_REPORT.md` | **new** | this report (PR description) |

No analytical function and no existing parser/formatter was modified.

---

## 6. How to use

Programmatic use:

```python
from www.services.standardizer import convert2df, validate, standardized_to_csv

df = convert2df("sources/Scopus/Scopus.csv", "scopus")   # -> standardized DataFrame
print(validate(df))                                       # -> {'valid': True, ...}
standardized_to_csv(df, "scopus_standardized.csv")        # flat CSV (lists joined by ';')
```

In the dashboard: choose "Import raw data file(s)", select the platform
(Scopus, Dimensions, PubMed, Lens, WoS, Cochrane), upload the corresponding raw
file. `get_data.py` now routes the file through `convert2df`, so the analyses
run on the standardized, strongly-typed DataFrame.

Reproduce the evidence:

```bash
python etl_demo.py
```

---

## 7. Scope

This submission targets the **BASE LEVEL**: standardization of manually
exported raw files and verified compatibility with the analytical functions.
The architecture (a dispatcher feeding a shared `transform`) was kept open so
that the ADVANCED LEVEL could later add an `api_retriever.py` module producing
raw records in the same shape and reusing `transform()`/`validate()` unchanged —
but no API code is included here, to keep the BASE-LEVEL deliverable minimal.
