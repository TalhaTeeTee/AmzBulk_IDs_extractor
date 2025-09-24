#!/usr/bin/env python3
import argparse
import re
import sys
from typing import List, Dict

import pandas as pd

SHEET_NAME = "Sponsored Products Campaigns"
DEFAULT_INPUT = "bulk.xlsx"
DEFAULT_OUTPUT = "SP_IDs.xlsx"

# ---------- Helpers ----------
def excel_col_to_idx(col_letter: str) -> int:
    col_letter = col_letter.strip().upper()
    n = 0
    for ch in col_letter:
        if not ('A' <= ch <= 'Z'):
            raise ValueError(f"Invalid column letter: {col_letter}")
        n = n * 26 + (ord(ch) - ord('A') + 1)
    return n - 1

def letters_to_indices(letters: List[str]) -> List[int]:
    return [excel_col_to_idx(l) for l in letters]

def pick_columns_by_letters(df: pd.DataFrame, letters: List[str]) -> pd.DataFrame:
    idxs = letters_to_indices(letters)
    if max(idxs, default=-1) >= df.shape[1]:
        missing = [letters[i] for i, idx in enumerate(idxs) if idx >= df.shape[1]]
        raise IndexError(f"Requested Excel columns {missing} not found in input.")
    return df.iloc[:, idxs].copy()  # keep original headers

def norm_text(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def match_entity(series: pd.Series, needle: str) -> pd.Series:
    s = norm_text(series).str.lower()
    if needle == "keyword":
        return s.eq("keyword") | s.str.startswith("keyword")
    if needle == "product targeting":
        return s.eq("product targeting") | s.str.startswith("product targeting")
    if needle == "product ad":
        return (
            s.eq("product ad") | s.eq("product ads") |
            s.str.startswith("product ad") | s.str.startswith("product ads")
        )
    return s.eq(needle)

def classify_ptx(series: pd.Series) -> Dict[str, pd.Series]:
    """Classify Product Targeting Expression (AJ) into PAT/Category/Auto."""
    s_raw = norm_text(series)
    s = s_raw.str.lower()

    # PAT: ASIN-like B0********
    pat_re = re.compile(r"\bB0[A-Z0-9]{8}\b", re.I)
    is_pat = s_raw.str.contains(pat_re, na=False)

    # Category
    is_category = s.str.contains("category", na=False)

    # Auto tokens
    auto_tokens = ("close", "loose", "substitute", "complement")
    is_auto = s.apply(lambda x: any(tok in x for tok in auto_tokens))

    return {"pat": is_pat, "category": is_category, "auto": is_auto}

def get_entity_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).strip().lower() == "entity":
            return c
    if df.shape[1] > 1:
        return df.columns[1]  # fallback to column B
    raise KeyError("Could not locate an 'Entity' column or fallback.")

# ---------- Core ----------
def build_maps(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    entity_col = get_entity_col(df)

    mask_keyword           = match_entity(df[entity_col], "keyword")
    mask_product_targeting = match_entity(df[entity_col], "product targeting")
    mask_product_ad        = match_entity(df[entity_col], "product ad")

    # Your column letter specs
    cols_keyword         = ["D", "E", "H", "L", "M", "R", "S", "T", "AC"]
    cols_prod_targeting  = ["D", "E", "I", "L", "M", "R", "S", "T", "AJ"]
    cols_product_ad      = ["D", "E", "G", "L", "M", "R", "S", "T", "W"]

    # Base maps
    KeywordTargetingMap  = pick_columns_by_letters(df.loc[mask_keyword], cols_keyword)
    ProductTargetingMap  = pick_columns_by_letters(df.loc[mask_product_targeting], cols_prod_targeting)
    AdvertisedProductMap = pick_columns_by_letters(df.loc[mask_product_ad], cols_product_ad)

    # ---- Classification happens on PRODUCT TARGETING rows using AJ ----
    aj_idx = excel_col_to_idx("AJ")
    if aj_idx >= df.shape[1]:
        print("WARNING: AJ column by position not found; classification maps will be empty.", file=sys.stderr)
        PATMap = ProductTargetingMap.iloc[0:0].copy()
        CategoryMap = ProductTargetingMap.iloc[0:0].copy()
        AutoMap = ProductTargetingMap.iloc[0:0].copy()
    else:
        pt_rows = df.loc[mask_product_targeting]
        aj_series = pt_rows.iloc[:, aj_idx]  # AJ == "Product Targeting Expression"
        masks = classify_ptx(aj_series)

        PAT_rows      = pt_rows[masks["pat"]]
        Category_rows = pt_rows[masks["category"]]
        Auto_rows     = pt_rows[masks["auto"]]

        # Keep the SAME columns layout as ProductTargetingMap for these three
        PATMap      = pick_columns_by_letters(PAT_rows, cols_prod_targeting)
        CategoryMap = pick_columns_by_letters(Category_rows, cols_prod_targeting)
        AutoMap     = pick_columns_by_letters(Auto_rows, cols_prod_targeting)

        # Diagnostics
        print(
            f"Product Targeting rows: {len(pt_rows)} | "
            f"PAT: {len(PATMap)} | Category: {len(CategoryMap)} | Auto: {len(AutoMap)}"
        )

    return {
        "1-SP-KeywordTargetingMap": KeywordTargetingMap,
        "2-SP-AdvertisedProductMap": AdvertisedProductMap,
        "3-SP-PATMap": PATMap,
        "4-SP-CategoryMap": CategoryMap,
        "5-SP-AutoMap": AutoMap,
    }

def main():
    ap = argparse.ArgumentParser(description="Extract SP maps from bulk.xlsx")
    ap.add_argument("--input", "-i", required=True,
                help="Path to input Excel (any filename, must contain the 'Sponsored Products Campaigns' sheet)")
    ap.add_argument("--output", "-o", default=DEFAULT_OUTPUT, help="Path to output Excel (default: SP_IDs.xlsx)")
    ap.add_argument("--sheet", "-s", default=SHEET_NAME, help=f"Sheet name (default: '{SHEET_NAME}')")
    args = ap.parse_args()

    try:
        df = pd.read_excel(args.input, sheet_name=args.sheet, dtype=str, engine="openpyxl")
    except Exception as e:
        print(f"ERROR: Failed to read '{args.input}' sheet '{args.sheet}': {e}", file=sys.stderr)
        sys.exit(1)

    maps = build_maps(df)

    try:
        with pd.ExcelWriter(args.output, engine="xlsxwriter") as xw:
            for name, mdf in maps.items():
                mdf.to_excel(xw, sheet_name=name[:31], index=False)
    except Exception as e:
        print(f"ERROR: Failed to write '{args.output}': {e}", file=sys.stderr)
        sys.exit(2)

    print(f"Created '{args.output}' with sheets:")
    for k in maps.keys():
        print(f" - {k}")

if __name__ == "__main__":
    main()
