# main.py
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import StreamingResponse, JSONResponse
import tempfile, io, os
from processor import process_bulk_to_xlsx

# Google Sheets push (optional)
USE_SHEETS = os.getenv("USE_SHEETS", "0") == "1"
if USE_SHEETS:
    import gspread
    from gspread_dataframe import set_with_dataframe
    import pandas as pd

app = FastAPI()

@app.get("/health")
async def health():
    return {"ok": True}

@app.post("/process-xlsx")
async def process_xlsx(file: UploadFile = File(...)):
    with tempfile.TemporaryDirectory() as tmp:
        in_path  = os.path.join(tmp, "input.xlsx")
        out_path = os.path.join(tmp, "SP_IDs.xlsx")
        with open(in_path, "wb") as f:
            f.write(await file.read())
        counts = process_bulk_to_xlsx(in_path, out_path)
        with open(out_path, "rb") as f:
            data = f.read()
    headers = {"Content-Disposition": "attachment; filename=SP_IDs.xlsx"}
    return StreamingResponse(io.BytesIO(data),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers)

@app.post("/process-to-sheets")
async def process_to_sheets(
    file: UploadFile = File(...),
    spreadsheet_id: str = Form(...),   # target Google Sheet
    tab_prefix: str = Form("SP")       # optional naming
):
    if not USE_SHEETS:
        return JSONResponse({"error": "Sheets disabled on server"}, status_code=400)

    with tempfile.TemporaryDirectory() as tmp:
        in_path  = os.path.join(tmp, "input.xlsx")
        out_path = os.path.join(tmp, "SP_IDs.xlsx")
        with open(in_path, "wb") as f:
            f.write(await file.read())
        counts = process_bulk_to_xlsx(in_path, out_path)

        # Also open locally and push individual sheets into a single Google Sheet
        # (requires service account creds in env var GOOGLE_APPLICATION_CREDENTIALS)
        gc = gspread.service_account()  # uses GOOGLE_APPLICATION_CREDENTIALS path
        sh = gc.open_by_key(spreadsheet_id)

        # Read back each sheet from the produced xlsx and write to Google Sheets tabs
        import pandas as pd
        x = pd.ExcelFile(out_path, engine="openpyxl")
        for sheet in x.sheet_names:
            df = x.parse(sheet)
            title = f"{tab_prefix}-{sheet}"[:99]
            try:
                ws = sh.worksheet(title)
                ws.clear()
            except Exception:
                ws = sh.add_worksheet(title=title, rows=max(len(df)+10, 200), cols=max(len(df.columns)+5, 26))
            set_with_dataframe(ws, df, include_index=False, include_column_header=True)

    return {"ok": True, "counts": counts, "spreadsheet_id": spreadsheet_id}