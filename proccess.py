# processor.py
import pandas as pd, re
from typing import Dict, List

SHEET_NAME = "Sponsored Products Campaigns"

def excel_col_to_idx(col_letter: str) -> int:
    col_letter = col_letter.strip().upper()
    n = 0
    for ch in col_letter: n = n*26 + (ord(ch)-64)
    return n-1

def letters_to_indices(letters: List[str]) -> List[int]:
    return [excel_col_to_idx(l) for l in letters]

def pick(df: pd.DataFrame, letters: List[str]) -> pd.DataFrame:
    idxs = letters_to_indices(letters)
    return df.iloc[:, idxs].copy()

def norm(s: pd.Series) -> pd.Series: return s.astype(str).str.strip()

def match_entity(series: pd.Series, needle: str) -> pd.Series:
    s = norm(series).str.lower()
    if needle == "keyword": return s.eq("keyword") | s.str.startswith("keyword")
    if needle == "product targeting": return s.eq("product targeting") | s.str.startswith("product targeting")
    if needle == "product ad":
        return s.eq("product ad")|s.eq("product ads")|s.str.startswith("product ad")|s.str.startswith("product ads")
    return s.eq(needle)

def entity_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).strip().lower() == "entity": return c
    return df.columns[1]

def classify_ptx(series: pd.Series):
    s_raw = norm(series); s = s_raw.str.lower()
    is_pat = s_raw.str.contains(re.compile(r"\bB0[A-Z0-9]{8}\b", re.I), na=False)
    is_category = s.str.contains("category", na=False)
    auto_tokens = ("close","loose","substitute","complement")
    is_auto = s.apply(lambda x: any(t in x for t in auto_tokens))
    return {"pat": is_pat, "category": is_category, "auto": is_auto}

def process_bulk_to_xlsx(input_path: str, output_path: str) -> Dict[str,int]:
    df = pd.read_excel(input_path, sheet_name=SHEET_NAME, dtype=str, engine="openpyxl")
    entc = entity_col(df)

    m_kw  = match_entity(df[entc], "keyword")
    m_pt  = match_entity(df[entc], "product targeting")
    m_ad  = match_entity(df[entc], "product ad")

    cols_kw = ["D","E","H","L","M","R","S","T","AC"]
    cols_pt = ["D","E","I","L","M","R","S","T","AJ"]
    cols_ad = ["D","E","G","L","M","R","S","T","W"]

    KW  = pick(df.loc[m_kw], cols_kw)
    PT  = pick(df.loc[m_pt], cols_pt)
    AD  = pick(df.loc[m_ad], cols_ad)

    aj = excel_col_to_idx("AJ")
    pt_rows = df.loc[m_pt]
    masks = classify_ptx(pt_rows.iloc[:, aj])

    PAT = pick(pt_rows[masks["pat"]],      cols_pt)
    CAT = pick(pt_rows[masks["category"]], cols_pt)
    AUT = pick(pt_rows[masks["auto"]],     cols_pt)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as xw:
        KW.to_excel(xw,  "1-SP-KeywordTargetingMap", index=False)
        AD.to_excel(xw,  "2-SP-AdvertisedProductMap", index=False)
        PAT.to_excel(xw, "3-SP-PATMap", index=False)
        CAT.to_excel(xw, "4-SP-CategoryMap", index=False)
        AUT.to_excel(xw, "5-SP-AutoMap", index=False)

    return {"KeywordTargetingMap": len(KW), "AdvertisedProductMap": len(AD),
            "PATMap": len(PAT), "CategoryMap": len(CAT), "AutoMap": len(AUT)}