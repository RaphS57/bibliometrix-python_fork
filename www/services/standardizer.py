"""
standardizer.py
===============

Source-agnostic ETL pipeline for Bibliometrix-Python (BASE LEVEL).

This module is the missing "spine" of the project. It plays the same role as
the ``convert2df()`` function of the R version of bibliometrix: it takes a raw
file manually exported from a bibliographic database (Scopus, Dimensions,
PubMed, Lens, Web of Science, Cochrane) and returns a single, standardized
pandas DataFrame that follows the internal Web of Science (WoS) schema used by
every analytical function in ``functions/`` and ``www/services/``.

The pipeline is split into the three mandatory sequential phases:

    EXTRACT   ->  read the raw file (pandas / rudimentary parsers)
    TRANSFORM ->  rename to WoS field tags + enforce strict type contracts
    LOAD      ->  add calculated fields (SR) + validate + return DataFrame

Design choices (see the project report for details):

* A single public entry point: :func:`convert2df`.
* A *dispatcher* (``SOURCE_ALIASES`` + :func:`extract`) routes each source to
  the correct reader, so the system is no longer implicitly tied to WoS.
* A *mapping dictionary* (``FORMATTERS``) centralizes the column mapping in one
  place instead of scattering it across the code base. The per-source parsing
  itself is delegated to the already-existing and already-tested
  ``format_*`` functions of ``format_functions.py`` (we reuse what works).
* *Type contracts* (``TYPE_CONTRACTS``) are enforced for every target column so
  that multi-value fields are real ``list[str]`` and no ``NaN``/``None`` value
  survives into the analytical functions.
* The Short Reference (SR) is **not** re-implemented here: we invoke the
  existing ``metaTagExtraction(df, "SR")`` function of ``metatagextraction.py``.
"""

from .utils import *
from .parsers import *
from .format_functions import *
from .metatagextraction import metaTagExtraction


# ---------------------------------------------------------------------------
# 0. TARGET SCHEMA, MAPPING DICTIONARY AND TYPE CONTRACTS

#Human-readable internal name expected by the ``format_*`` functions, keyed by
#the short source identifier used in the dashboard ("wos", "scopus", ...).
SOURCE_ALIASES = {
    "wos": "Web_of_Science",
    "scopus": "Scopus",
    "dimensions": "Dimensions",
    "lens": "The_Lens",
    "pubmed": "PubMed",
    "cochrane": "Cochrane",
}

#Provenance label written to the DB column (used by downstream functions to
#check where the data comes from, e.g. SR() behaves differently for Scopus).
DB_LABELS = {
    "wos": "WEB_OF_SCIENCE",
    "scopus": "SCOPUS",
    "dimensions": "DIMENSIONS",
    "lens": "LENS",
    "pubmed": "PUBMED",
    "cochrane": "COCHRANE",
}

#Mapping dictionary / "Lookup Strategy": target WoS field tag -> the existing
#function able to extract and format that field for ANY source. This is the
#single, centralized place where the raw data is mapped to the WoS schema.
FORMATTERS = {
    "AB": format_ab_column,    # Abstract
    "AF": format_af_column,    # Author full names
    "AU": format_au_column,    # Authors
    "C1": format_c1_column,    # Author affiliations
    "CR": format_cr_column,    # Cited references
    "DE": format_de_column,    # Author keywords
    "DI": format_di_column,    # DOI
    "DT": format_dt_column,    # Document type
    "ID": format_id_column,    # Index keywords (Keywords Plus)
    "IS": format_is_column,    # Issue
    "JI": format_ji_column,    # ISO source abbreviation
    "LA": format_la_column,    # Language
    "BP": format_bp_column,    # Beginning page
    "EP": format_ep_column,    # Ending page
    "PMID": format_pmid_column,  # PubMed ID
    "PY": format_py_column,    # Publication year
    "RP": format_rp_column,    # Reprint / correspondence address
    "SO": format_so_column,    # Source / journal
    "TC": format_tc_column,    # Times cited
    "TI": format_ti_column,    # Title
    "UT": format_ut_column,    # Unique article identifier
    "VL": format_vl_column,    # Volume
    "AU_UN": format_au_un_column,  # Author universities (helper, extra)
}

#Type contract for every column of the target schema.
# list -> multi-value field, must be list[str], null -> []
# int -> numeric scalar, null -> 0
# str -> scalar text, null -> ""
TYPE_CONTRACTS = {
    "DB": str, "UT": str, "DI": str, "PMID": str, "TI": str, "SO": str,
    "JI": str, "DT": str, "LA": str, "RP": str, "AB": str, "VL": str,
    "IS": str, "BP": str, "EP": str, "SR": str,
    "PY": int, "TC": int,
    "AU": list, "AF": list, "C1": list, "CR": list, "DE": list, "ID": list,
    "AU_UN": list,  # helper column kept for collaboration analyses
}

#Mandatory columns of the target schema (the glossary of section 4.2 of the
#assignment). The validation step guarantees that all of them exist.
MANDATORY_COLUMNS = [
    "DB", "UT", "DI", "PMID", "TI", "SO", "JI", "PY", "DT", "LA", "TC",
    "AU", "AF", "C1", "RP", "CR", "DE", "ID", "AB", "VL", "IS", "BP", "EP",
    "SR",
]


# ---------------------------------------------------------------------------
# 1. EXTRACT

def _detect_file_type(filename):
    """Return the lowercase file extension (e.g. ``.csv``) of a file name."""
    return os.path.splitext(filename)[1].lower()


def extract(filepath, source, filename=None):
    """
    EXTRACT phase: read a raw exported file into a list of raw record dicts.

    The reader is chosen by a *dispatcher* based on the source and the file
    extension. Tabular formats are read with ``pandas`` (``read_csv`` /
    ``read_excel``); text formats are read with the rudimentary parsers of
    ``parsers.py``. No transformation is applied here.

    Args:
        filepath (str): Path to the raw file on disk.
        source (str): Short source id ("scopus", "dimensions", "pubmed",
            "lens", "wos", "cochrane").
        filename (str, optional): Original file name, used to detect the
            extension when ``filepath`` has none. Defaults to ``filepath``.

    Returns:
        tuple[list[dict], str]: ``(raw_records, file_type)`` where
        ``file_type`` is the detected extension (e.g. ``".csv"``).

    Raises:
        ValueError: If the source/extension combination is not supported.
    """
    source = source.lower()
    if source not in SOURCE_ALIASES:
        raise ValueError(f"Unknown source '{source}'. "
                         f"Supported: {sorted(SOURCE_ALIASES)}")

    file_type = _detect_file_type(filename or filepath)

    #Tabular sources (pandas)
    if source == "scopus" and file_type == ".csv":
        records = pd.read_csv(filepath).to_dict(orient="records")
    elif source == "lens" and file_type == ".csv":
        records = pd.read_csv(filepath).to_dict(orient="records")
    elif source == "dimensions" and file_type == ".csv":
        #Dimensions CSV exports have a 1-line banner before the header
        records = pd.read_csv(filepath, skiprows=1).to_dict(orient="records")
    elif source == "dimensions" and file_type == ".xlsx":
        records = pd.read_excel(filepath, skiprows=1).to_dict(orient="records")

    #Text sources (rudimentary parsers)
    elif source == "wos" and file_type in (".txt", ".ciw"):
        records = parse_wos_data(filepath)
    elif source == "pubmed" and file_type == ".txt":
        records = parse_pubmed_data(filepath)
    elif source == "cochrane" and file_type == ".txt":
        records = parse_cochrane_data(filepath)
    else:
        raise ValueError(
            f"Unsupported combination: source='{source}', file_type='{file_type}'."
        )

    return records, file_type


# ---------------------------------------------------------------------------
# 2. TRANSFORM (rename + type contracts + null handling)


def _clean_list(value):
    """Coerce any value into a clean ``list[str]`` (drop null/empty items)."""
    if isinstance(value, list):
        items = value
    elif value is None or (isinstance(value, float) and math.isnan(value)):
        items = []
    else:
        # A flat, semicolon-delimited string is split back into a list.
        items = str(value).split(";")

    cleaned = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, float) and math.isnan(item):
            continue
        text = str(item).strip()
        if text and text.lower() not in ("nan", "none"):
            cleaned.append(text)
    return cleaned


def _clean_int(value):
    """Coerce any value into an ``int`` (null / non-numeric -> 0)."""
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return 0
    return int(number)


def _clean_str(value):
    """Coerce any value into a clean ``str`` (null -> "")."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, list):
        value = "; ".join(str(v) for v in value)
    text = str(value).strip()
    if text.lower() in ("nan", "none"):
        return ""
    return text


def _enforce_contract(value, expected_type):
    """Apply the type contract for a single cell."""
    if expected_type is list:
        return _clean_list(value)
    if expected_type is int:
        return _clean_int(value)
    return _clean_str(value)


def transform(raw_records, source, file_type):
    """
    TRANSFORM phase: map raw records to the WoS schema and enforce type
    contracts.

    For each raw record the centralized ``FORMATTERS`` mapping dictionary is
    applied to obtain every target column (reusing the existing per-source
    ``format_*`` functions). The strict ``TYPE_CONTRACTS`` are then enforced so
    that multi-value fields become ``list[str]``, numeric fields become ``int``
    and no ``NaN``/``None`` value survives.

    Args:
        raw_records (list[dict]): Output of :func:`extract`.
        source (str): Short source id.
        file_type (str): Detected file extension (e.g. ``".csv"``).

    Returns:
        pandas.DataFrame: A DataFrame with the standardized columns
        (SR is still empty here, it is computed in the LOAD phase).
    """
    source = source.lower()
    internal_source = SOURCE_ALIASES[source]
    db_label = DB_LABELS[source]

    rows = []
    for entry in raw_records:
        row = {"DB": db_label, "SR": ""}
        for tag, formatter in FORMATTERS.items():
            try:
                row[tag] = formatter(entry, internal_source, file_type)
            except Exception:
                #A single malformed field must never crash the whole pipeline:
                #fall back to an empty value, the type contract will fix it
                row[tag] = None
        rows.append(row)

    df = pd.DataFrame(rows)

    #Guarantee that every mandatory column exists, even if a source provides
    #no data for it (the column is created empty and typed below)
    for col in MANDATORY_COLUMNS:
        if col not in df.columns:
            df[col] = None

    #Enforce the type contract column by column
    for col, expected_type in TYPE_CONTRACTS.items():
        if col in df.columns:
            df[col] = df[col].apply(lambda v: _enforce_contract(v, expected_type))

    return df


# ---------------------------------------------------------------------------
# 3. LOAD (calculated fields + validation)


class _DataHolder:
    """Minimal stand-in for the Shiny reactive value used by the services.

    ``metaTagExtraction`` expects an object exposing ``.get()`` / ``.set()``.
    Outside the dashboard we wrap a plain DataFrame in this tiny holder so we
    can reuse the existing implementation unchanged.
    """

    def __init__(self, df):
        self._df = df

    def get(self):
        return self._df

    def set(self, df):
        self._df = df


def add_calculated_fields(df):
    """
    CALCULATED FIELDS phase: build the Short Reference (SR).

    We do not re-implement SR: we invoke the existing ``metaTagExtraction``
    service (``services/metatagextraction.py``), which produces the canonical
    ``SR`` (with cross-corpus disambiguation) and ``SR_FULL`` columns.

    Args:
        df (pandas.DataFrame): Standardized DataFrame from :func:`transform`.

    Returns:
        pandas.DataFrame: The same DataFrame with ``SR`` (and ``SR_FULL``).
    """
    holder = _DataHolder(df)
    holder = metaTagExtraction(holder, "SR")
    df = holder.get()
    #The SR column is the only multi-value-free key we must re-contract
    df["SR"] = df["SR"].apply(_clean_str)
    if "SR_FULL" in df.columns:
        df["SR_FULL"] = df["SR_FULL"].apply(_clean_str)
    return df


def validate(df, raise_on_error=False):
    """
    VALIDATION phase: programmatically verify the output contract.

    Checks performed:
        1. All mandatory columns exist.
        2. No ``NaN`` / ``None`` value remains in any cell.
        3. Multi-value columns are typed as ``list``.
        4. Numeric columns (PY, TC) are integers.

    Args:
        df (pandas.DataFrame): The standardized DataFrame.
        raise_on_error (bool): If True, raise ``ValueError`` on the first
            failure instead of only reporting it.

    Returns:
        dict: A report ``{"valid": bool, "errors": [...], "n_rows": int}``.
    """
    errors = []

    #1. Mandatory columns
    missing = [c for c in MANDATORY_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"Missing mandatory columns: {missing}")

    #2 / 3 & 4. Per-column type and null checks
    for col, expected_type in TYPE_CONTRACTS.items():
        if col not in df.columns:
            continue
        if expected_type is list:
            bad = df[col].apply(lambda v: not isinstance(v, list)).sum()
            if bad:
                errors.append(f"Column '{col}' has {bad} non-list values.")
        elif expected_type is int:
            bad = df[col].apply(lambda v: not isinstance(v, (int, np.integer))).sum()
            if bad:
                errors.append(f"Column '{col}' has {bad} non-int values.")
            if df[col].isna().any():
                errors.append(f"Column '{col}' still contains NaN.")
        else:  # str
            bad = df[col].apply(lambda v: not isinstance(v, str)).sum()
            if bad:
                errors.append(f"Column '{col}' has {bad} non-str values.")

    report = {"valid": len(errors) == 0, "errors": errors, "n_rows": len(df)}
    if raise_on_error and errors:
        raise ValueError("Validation failed: " + "; ".join(errors))
    return report


# ---------------------------------------------------------------------------
# 4. PUBLIC ENTRY POINT

def convert2df(filepath, source, filename=None, validate_output=True):
    """
    Single entry point of the ETL pipeline (Python analogue of R's
    ``convert2df()``).

    It chains the three mandatory phases:

        EXTRACT   -> :func:`extract`
        TRANSFORM -> :func:`transform`
        LOAD      -> :func:`add_calculated_fields` + :func:`validate`

    Args:
        filepath (str): Path to the raw exported file.
        source (str): Short source id ("scopus", "dimensions", "pubmed",
            "lens", "wos", "cochrane").
        filename (str, optional): Original file name (for extension detection).
        validate_output (bool): If True (default) run the validation step.

    Returns:
        pandas.DataFrame: A standardized, analysis-ready DataFrame.
    """
    raw_records, file_type = extract(filepath, source, filename=filename)
    df = transform(raw_records, source, file_type)
    df = add_calculated_fields(df)
    if validate_output:
        report = validate(df)
        if not report["valid"]:
            # We warn but do not crash: BASE LEVEL favours a usable DataFrame
            print("[standardizer] validation warnings:", report["errors"])
    return df


def standardized_to_csv(df, output_path):
    """
    Serialize a standardized DataFrame to a flat CSV file.

    Args:
        df (pandas.DataFrame): The standardized DataFrame.
        output_path (str): Destination CSV path.

    Returns:
        str: ``output_path``.
    """
    flat = df.copy()
    for col, expected_type in TYPE_CONTRACTS.items():
        if expected_type is list and col in flat.columns:
            flat[col] = flat[col].apply(
                lambda v: ";".join(v) if isinstance(v, list) else ""
            )
    flat.to_csv(output_path, index=False)
    return output_path
