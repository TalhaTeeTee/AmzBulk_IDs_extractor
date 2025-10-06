import streamlit as st
import pandas as pd
import re
import io
from typing import Dict, List

# Set page config
st.set_page_config(
    page_title="Amazon Ads Bulk Processor",
    page_icon="📊",
    layout="wide"
)

SHEET_NAME = "Sponsored Products Campaigns"

# ---------- Helper Functions ----------
def excel_col_to_idx(col_letter: str) -> int:
    col_letter = col_letter.strip().upper()
    n = 0
    for ch in col_letter:
        n = n * 26 + (ord(ch) - 64)
    return n - 1

def letters_to_indices(letters: List[str]) -> List[int]:
    return [excel_col_to_idx(l) for l in letters]

def pick(df: pd.DataFrame, letters: List[str]) -> pd.DataFrame:
    idxs = letters_to_indices(letters)
    return df.iloc[:, idxs].copy()

def norm(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def match_entity(series: pd.Series, needle: str) -> pd.Series:
    s = norm(series).str.lower()
    if needle == "keyword":
        return s.eq("keyword") | s.str.startswith("keyword")
    if needle == "product targeting":
        return s.eq("product targeting") | s.str.startswith("product targeting")
    if needle == "product ad":
        return s.eq("product ad") | s.eq("product ads") | s.str.startswith("product ad") | s.str.startswith("product ads")
    return s.eq(needle)

def entity_col(df: pd.DataFrame) -> str:
    for c in df.columns:
        if str(c).strip().lower() == "entity":
            return c
    return df.columns[1]

def classify_ptx(series: pd.Series):
    s_raw = norm(series)
    s = s_raw.str.lower()
    is_pat = s_raw.str.contains(re.compile(r"\bB0[A-Z0-9]{8}\b", re.I), na=False)
    is_category = s.str.contains("category", na=False)
    auto_tokens = ("close", "loose", "substitute", "complement")
    is_auto = s.apply(lambda x: any(t in x for t in auto_tokens))
    return {"pat": is_pat, "category": is_category, "auto": is_auto}

def process_bulk_file(uploaded_file) -> tuple:
    """Process the uploaded bulk file and return Excel bytes + stats"""
    try:
        # Read the Excel file
        df = pd.read_excel(uploaded_file, sheet_name=SHEET_NAME, dtype=str, engine="openpyxl")
        
        entc = entity_col(df)
        
        # Match entities
        m_kw = match_entity(df[entc], "keyword")
        m_pt = match_entity(df[entc], "product targeting")
        m_ad = match_entity(df[entc], "product ad")
        
        # Define columns
        cols_kw = ["D", "E", "H", "L", "M", "R", "S", "T", "AC"]
        cols_pt = ["D", "E", "I", "L", "M", "R", "S", "T", "AJ"]
        cols_ad = ["D", "E", "G", "L", "M", "R", "S", "T", "W"]
        
        # Extract base dataframes
        KW = pick(df.loc[m_kw], cols_kw)
        PT = pick(df.loc[m_pt], cols_pt)
        AD = pick(df.loc[m_ad], cols_ad)
        
        # Classify product targeting
        aj = excel_col_to_idx("AJ")
        pt_rows = df.loc[m_pt]
        
        if len(pt_rows) > 0 and aj < df.shape[1]:
            masks = classify_ptx(pt_rows.iloc[:, aj])
            PAT = pick(pt_rows[masks["pat"]], cols_pt)
            CAT = pick(pt_rows[masks["category"]], cols_pt)
            AUT = pick(pt_rows[masks["auto"]], cols_pt)
        else:
            PAT = PT.iloc[0:0].copy()
            CAT = PT.iloc[0:0].copy()
            AUT = PT.iloc[0:0].copy()
        
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            KW.to_excel(writer, sheet_name="1-SP-KeywordTargetingMap", index=False)
            AD.to_excel(writer, sheet_name="2-SP-AdvertisedProductMap", index=False)
            PAT.to_excel(writer, sheet_name="3-SP-PATMap", index=False)
            CAT.to_excel(writer, sheet_name="4-SP-CategoryMap", index=False)
            AUT.to_excel(writer, sheet_name="5-SP-AutoMap", index=False)
        
        output.seek(0)
        
        # Return stats
        stats = {
            "Keyword Targeting": len(KW),
            "Advertised Products": len(AD),
            "PAT (Product Attribute Targeting)": len(PAT),
            "Category Targeting": len(CAT),
            "Auto Targeting": len(AUT),
            "Total Rows Processed": len(df)
        }
        
        return output.getvalue(), stats, None
        
    except Exception as e:
        return None, None, str(e)

# ---------- Streamlit UI ----------
def main():
    st.title("📊 Amazon Ads Bulk File Processor")
    st.markdown("---")
    
    st.markdown("""
    ### How to use:
    1. Upload your Amazon Ads bulk Excel file (must contain a sheet named "Sponsored Products Campaigns")
    2. Click **Process File**
    3. Download the processed file with 5 organized sheets
    """)
    
    st.markdown("---")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Upload your Amazon Ads Bulk File (.xlsx)",
        type=["xlsx"],
        help="The file must contain a sheet named 'Sponsored Products Campaigns'"
    )
    
    if uploaded_file is not None:
        st.success(f"✅ File uploaded: {uploaded_file.name}")
        
        # Process button
        if st.button("🚀 Process File", type="primary"):
            with st.spinner("Processing your file... This may take a moment."):
                excel_bytes, stats, error = process_bulk_file(uploaded_file)
                
                if error:
                    st.error(f"❌ Error processing file: {error}")
                    st.info("Please ensure your file contains a sheet named 'Sponsored Products Campaigns' with the expected format.")
                else:
                    st.success("✅ File processed successfully!")
                    
                    # Display statistics
                    st.markdown("### 📈 Processing Results")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric("Total Rows", stats["Total Rows Processed"])
                        st.metric("Keyword Targeting", stats["Keyword Targeting"])
                        st.metric("Advertised Products", stats["Advertised Products"])
                    
                    with col2:
                        st.metric("PAT", stats["PAT (Product Attribute Targeting)"])
                        st.metric("Category Targeting", stats["Category Targeting"])
                        st.metric("Auto Targeting", stats["Auto Targeting"])
                    
                    # Download button
                    st.markdown("---")
                    st.download_button(
                        label="⬇️ Download Processed File",
                        data=excel_bytes,
                        file_name="SP_IDs.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                    
                    st.markdown("### 📋 Output Sheets")
                    st.markdown("""
                    Your processed file contains 5 sheets:
                    1. **1-SP-KeywordTargetingMap** - Keyword targeting data
                    2. **2-SP-AdvertisedProductMap** - Advertised product data
                    3. **3-SP-PATMap** - Product Attribute Targeting
                    4. **4-SP-CategoryMap** - Category targeting data
                    5. **5-SP-AutoMap** - Auto targeting data
                    """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <small>Amazon Ads Bulk Processor | Built with Streamlit</small>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
