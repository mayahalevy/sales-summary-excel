# -*- coding: utf-8 -*-
"""
Interactive all-in-one (no intermediates):
Reads an Excel file, filters by date & status across all tabs,
and writes a single summary CSV (Hebrew headers) next to the Excel file.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import parser as dtparser
from pathlib import Path
import platform, subprocess, os

# Hebrew column names
COL_STATUS       = 'סטטוס'
COL_ITEM_NAME    = 'שם פריט'
COL_ITEM_TYPE    = 'סוג פריט'
COL_SALE_DATE    = 'תאריך מכירה'
COL_SALE_TIME    = 'שעת מכירה'
COL_CUSTOMER_TYP = 'סוג לקוח'
COL_REP          = 'נציג'
COL_NOTES        = 'הערות'
COL_COMMISSION   = 'עמלה'
COL_UPSALE       = 'UPSALE'
COL_BRANCH       = 'סניף'

REQUIRED_MIN = [COL_STATUS, COL_ITEM_NAME, COL_ITEM_TYPE, COL_SALE_DATE, COL_SALE_TIME, COL_CUSTOMER_TYP]
OUTPUT_COLS  = [COL_ITEM_NAME, COL_ITEM_TYPE, COL_SALE_DATE, COL_SALE_TIME, COL_CUSTOMER_TYP,
                COL_REP, COL_NOTES, COL_COMMISSION, COL_UPSALE, COL_BRANCH]

# Tabs to skip
EXCLUDED_TABS = {'עמלת מכירה', 'ניהול מחסנאי', 'ניהול ראשי'}

def norm_date_series(s):
    out = pd.to_datetime(s, errors='coerce', dayfirst=True)
    is_num = pd.to_numeric(s, errors='coerce')
    mask_num = is_num.notna() & out.isna()
    if mask_num.any():
        base = pd.Timestamp('1899-12-30')
        out.loc[mask_num] = base + pd.to_timedelta(is_num[mask_num], unit='D')
    return out.dt.normalize()

def excel_time_to_str(val):
    if pd.isna(val):
        return 0, ""
    if isinstance(val, (pd.Timestamp, datetime)):
        t = val.time()
        sec = t.hour*3600 + t.minute*60 + t.second
        return sec, f"{t.hour:02d}:{t.minute:02d}"
    if isinstance(val, str):
        try:
            dt = dtparser.parse(val)
            return dt.hour*3600 + dt.minute*60 + dt.second, f"{dt.hour:02d}:{dt.minute:02d}"
        except Exception:
            return 0, val
    if isinstance(val, (int, float, np.floating)) and 0 <= float(val) < 2:
        seconds = float(val)*24*3600
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        return h*3600 + m*60 + s, f"{h:02d}:{m:02d}"
    return 0, str(val)

def open_folder(path: Path):
    try:
        if platform.system() == "Windows":
            os.startfile(str(path))
        elif platform.system() == "Darwin":
            subprocess.run(["open", str(path)])
        else:
            subprocess.run(["xdg-open", str(path)])
    except Exception:
        pass

def process_excel(xlsx_path, start_str, end_str, status="נמכר", open_when_done=True):
    start = dtparser.parse(start_str, dayfirst=True).replace(hour=0, minute=0, second=0, microsecond=0)
    end   = dtparser.parse(end_str,   dayfirst=True).replace(hour=23, minute=59, second=59, microsecond=999999)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    xlsx_path = Path(xlsx_path)
    excel_dir = xlsx_path.parent
    out_summary = excel_dir / f"summary_{ts}.csv"

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    print(f"\n📘 Loaded {len(xls.sheet_names)} tabs from {xlsx_path}\n")

    collected = []

    for sheet in xls.sheet_names:
        if sheet in EXCLUDED_TABS or sheet.startswith("Summary_"):
            print(f"⏩ Skipping admin tab: {sheet}")
            continue

        df = pd.read_excel(xls, sheet_name=sheet)
        if df.empty:
            continue

        df.columns = [str(c).strip() for c in df.columns]
        if not all(c in df.columns for c in REQUIRED_MIN):
            print(f"⚠️  Missing required columns in {sheet}, skipping.")
            continue

        sale_dates = norm_date_series(df[COL_SALE_DATE])
        status_ok = df[COL_STATUS].astype(str).str.strip() == status
        date_ok   = (sale_dates >= pd.Timestamp(start.date())) & (sale_dates <= pd.Timestamp(end.date()))
        mask = status_ok & date_ok
        if not mask.any():
            continue

        sub = df.loc[mask, [COL_ITEM_NAME, COL_ITEM_TYPE, COL_SALE_DATE, COL_SALE_TIME, COL_CUSTOMER_TYP]].copy()
        for opt in [COL_REP, COL_NOTES, COL_COMMISSION, COL_UPSALE]:
            sub[opt] = df[opt] if opt in df.columns else ""

        sub[COL_BRANCH] = sheet
        sub["_date_sort"] = norm_date_series(sub[COL_SALE_DATE])

        tsec, tdisp = [], []
        for v in sub[COL_SALE_TIME]:
            sec, disp = excel_time_to_str(v)
            tsec.append(sec); tdisp.append(disp)
        sub["_time_sec"] = tsec
        sub[COL_SALE_TIME] = tdisp

        collected.append(sub)

    if not collected:
        pd.DataFrame(columns=OUTPUT_COLS).to_csv(out_summary, index=False, encoding="utf-8-sig")
        print(f"\n⚠️ No matching rows found. Wrote empty summary → {out_summary}\n")
        if open_when_done: open_folder(excel_dir)
        return

    all_rows = pd.concat(collected, ignore_index=True)
    all_rows.sort_values(by=["_date_sort", "_time_sec"], ascending=[False, False], inplace=True)
    all_rows.drop(columns=["_date_sort", "_time_sec"], inplace=True)

    for col in OUTPUT_COLS:
        if col not in all_rows.columns:
            all_rows[col] = ""
    all_rows = all_rows[OUTPUT_COLS]

    all_rows.to_csv(out_summary, index=False, encoding="utf-8-sig")
    print(f"\n✅ Wrote summary ({len(all_rows)} rows) → {out_summary}\n")
    if open_when_done: open_folder(excel_dir)

if __name__ == "__main__":
    print("📊 Excel → single summary CSV (no intermediate files)\n")
    xlsx_path = input("📄 Enter path to Excel file (or drag it here): ").strip().strip('"')
    start_date = input("🗓️  Start date (dd/mm/yyyy or yyyy-mm-dd): ").strip()
    end_date   = input("🗓️  End date (dd/mm/yyyy or yyyy-mm-dd): ").strip()
    status     = input("📦 Status filter (default: נמכר): ").strip() or "נמכר"
    process_excel(xlsx_path, start_date, end_date, status, open_when_done=True)
