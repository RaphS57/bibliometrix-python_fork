"""
etl_demo.py
===========

Execution evidence for the BASE LEVEL ETL pipeline.

Run from the project root:

    python etl_demo.py
"""

import os
from www.services.standardizer import convert2df, validate, standardized_to_csv

#(short source id, raw file path) for the files.
DATASETS = [
    ("scopus", "sources/Scopus/Scopus.csv"),
    ("dimensions", "sources/Dimensions/Dimensions.xlsx"),
    ("lens", "sources/Lens/Lens.csv"),
    ("pubmed", "sources/PubMed/pubmed-allergicrh-set.txt"),
    ("wos", "sources/Web_of_Science/WoS.txt"),
]

PREVIEW_COLS = ["DB", "TI", "PY", "TC", "SO", "AU", "DE", "CR", "SR"]


def main():
    out_dir = os.path.join("sources", "standardized")
    os.makedirs(out_dir, exist_ok=True)

    for source, path in DATASETS:
        print("=" * 78)
        print(f"SOURCE: {source}   FILE: {path}")
        if not os.path.exists(path):
            print("  (file not found, skipped)")
            continue

        #EXTRACT + TRANSFORM + LOAD
        df = convert2df(path, source)

        #VALIDATION
        report = validate(df)
        print(f"  rows={report['n_rows']}  valid={report['valid']}")
        if report["errors"]:
            print("  errors:", report["errors"])
        print(f"  PY dtype={df['PY'].dtype}  TC dtype={df['TC'].dtype}")

        #PREVIEW
        print("  first standardized row:")
        row = df.iloc[0]
        for col in PREVIEW_COLS:
            value = row[col]
            if isinstance(value, list):
                value = value[:3]
            print(f"    {col:5} : {str(value)[:80]}")

        #WRITE STANDARDIZED CSV
        out_csv = os.path.join(out_dir, f"{source}_standardized.csv")
        standardized_to_csv(df, out_csv)
        print(f"  standardized CSV written to: {out_csv}")

    print("=" * 78)
    print("Done.")


if __name__ == "__main__":
    main()
