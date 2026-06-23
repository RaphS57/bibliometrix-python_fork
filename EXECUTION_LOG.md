# Execution Log — ETL Pipeline (BASE LEVEL)

This log records the execution evidence of the source-agnostic ETL pipeline
(`www/services/standardizer.py`). It shows (1) the standardization of raw files
from five bibliographic databases, and (2) the successful execution of a
representative set of analytical functions on the standardized DataFrames.

## 1. Standardization (`convert2df`)

Each raw file was processed with `convert2df(path, source)` and passed the
validation module with no errors. `PY` and `TC` are cast to `int`; the
multi-value fields (`AU`, `AF`, `C1`, `CR`, `DE`, `ID`) are real `list[str]`.

| Source      | Raw file (sample)            | Rows | Validation | PY dtype | TC dtype |
|-------------|------------------------------|------|------------|----------|----------|
| Scopus      | `Scopus.csv`                 | 60   | valid      | int64    | int64    |
| Dimensions  | `Dimensions.csv` (skiprows=1)| 28   | valid      | int64    | int64    |
| Lens        | `Lens.csv`                   | 60   | valid      | int64    | int64    |
| PubMed      | `pubmed-allergicrh-set.txt`  | 18   | valid      | int64    | int64    |
| Web of Sci. | `WoS.txt`                    | 36   | valid      | int64    | int64    |

Standardized columns produced (24 mandatory + 2 helpers `AU_UN`, `SR_FULL`):

```
DB, SR, AB, AF, AU, C1, CR, DE, DI, DT, ID, IS, JI, LA, BP, EP,
PMID, PY, RP, SO, TC, TI, UT, VL, AU_UN, SR_FULL
```

Example standardized row (Scopus):

```
DB    : SCOPUS
SR    : Woldegeorgis B.Z., 2024, BMC Infect Dis
PY    : 2024            (int)
TC    : 0               (int)
SO    : BMC Infectious Diseases
AU    : ['Woldegeorgis B.Z.', 'Asgedom Y.S.', ...]      (list[str])
DE    : ['Antiretroviral therapy', 'Children', ...]     (list[str])
CR    : ['(2023) ...', ...]                              (list[str])
```

Flat standardized CSVs (list fields joined with `;`) are written to
`sources/standardized/` by `etl_demo.py`.

## 2. Analytical-function compatibility matrix

Each function was run on the standardized DataFrame of every source.
`PASS` = the function executed end-to-end without raising.

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

**Result: 40 / 40 executions passed.** No analytical function had to be
patched: standardizing the data (correct column names, `list` types, integer
`PY`/`TC`, no `NaN`) was sufficient to make the WoS-only functions work for
Scopus, Dimensions, Lens and PubMed.

## 3. How to reproduce

From the project root, in the full environment (with the dashboard
dependencies installed):

```bash
python etl_demo.py
```

This standardizes every shipped dataset, prints the validation report and the
first rows, and writes the flat standardized CSVs to `sources/standardized/`.
