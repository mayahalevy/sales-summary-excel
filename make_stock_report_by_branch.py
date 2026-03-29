# -*- coding: utf-8 -*-
"""
Stock report pivot:
Rows  = item name (שם פריט)
Cols  = one column per branch (sheet name, e.g. 'סניף רמת גן', 'סניף תל אביב', ...)
Value = count of rows where סטטוס = חדש
Plus a 'סה"כ' column that sums across all branches.
"""

import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd

COL_STATUS = 'סטטוס'
COL_ITEM   = 'שם פריט'
COL_BRANCH = 'סניף'
DEFAULT_STATUS = 'חדש'

# Tabs to skip (adjust as needed)
EXCLUDED_TABS = {'עמלת מכירה', 'ניהול מחסנאי', 'ניהול ראשי'}

def normalize_text(s):
    if pd.isna(s):
        return ""
    s = str(s)
    for ch in ["\u200f", "\u200e", "\u202a", "\u202b", "\u202c",
               "\u202d", "\u202e", "\xa0"]:
        s = s.replace(ch, " ")
    return s.strip()

def parse_args():
    ap = argparse.ArgumentParser(
        description="Pivot stock report by item (שם פריט) with one column per branch and total."
    )
    ap.add_argument("xlsx", help="Path to the Excel file (.xlsx)")
    ap.add_argument("--status", default=DEFAULT_STATUS,
                    help="Status to include (default: חדש)")
    ap.add_argument("--status-col", default=COL_STATUS,
                    help="Status column name (default: סטטוס)")
    ap.add_argument("--item-col", default=COL_ITEM,
                    help="Item name column (default: שם פריט)")
    ap.add_argument("--output", default=None,
                    help="Output CSV path (default: stock_pivot_YYYYMMDD_HHMMSS.csv)")
    return ap.parse_args()

def main():
    args = parse_args()

    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel not found: {xlsx_path}")

    excel_dir = xlsx_path.parent
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv = Path(args.output) if args.output else (
        excel_dir / f"stock_pivot_{ts}.csv"
    )

    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    frames = []

    for sheet in xls.sheet_names:
        # Skip non-branch / summary tabs
        if sheet in EXCLUDED_TABS or sheet.startswith("Summary_"):
            continue

        df = pd.read_excel(xls, sheet_name=sheet)
        if df.empty:
            continue

        # Normalize headers
        df.columns = [normalize_text(c) for c in df.columns]

        # Require status + item columns
        if args.status_col not in df.columns or args.item_col not in df.columns:
            continue

        # Filter by סטטוס ~ "חדש" (contains, forgiving)
        status_series = df[args.status_col].astype(str).map(normalize_text)
        mask = status_series.str.contains(args.status, case=False, regex=False, na=False)
        df = df.loc[mask].copy()
        if df.empty:
            continue

        # Keep רק שם פריט + סניף (branch)
        trimmed = df[[args.item_col]].copy()
        trimmed[COL_BRANCH] = sheet

        frames.append(trimmed)

    if not frames:
        # No matches anywhere → write empty structure
        pd.DataFrame(columns=[COL_ITEM, "סה\"כ"]).to_csv(
            out_csv, index=False, encoding="utf-8-sig"
        )
        print(f"⚠️ No rows with status '{args.status}'. Wrote empty file → {out_csv}")
        return

    all_rows = pd.concat(frames, ignore_index=True)

    # Add quantity column = 1 per row to pivot on
    all_rows["__כמות"] = 1

    # Pivot: index = item, columns = branch, values = sum of quantity
    pivot = (
        all_rows
        .pivot_table(
            index=args.item_col,
            columns=COL_BRANCH,
            values="__כמות",
            aggfunc="sum",
            fill_value=0
        )
    )

    # Flatten columns (in case of MultiIndex) and turn index back to column
    pivot = pivot.reset_index()

    # Sort branch columns alphabetically (optional)
    # First col is item; rest are branches
    cols = list(pivot.columns)
    item_col = cols[0]
    branch_cols = sorted(cols[1:], key=lambda c: str(c))
    pivot = pivot[[item_col] + branch_cols]

    # Add total column (sum across all branch columns)
    pivot['סה"כ'] = pivot[branch_cols].sum(axis=1)

    # Save to CSV
    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel not found: {xlsx_path}")

    # Always save output next to the Excel file
    excel_dir = xlsx_path.parent
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Create filename automatically, unless user overrides with --output
    out_csv = (
        Path(args.output)
        if args.output
        else excel_dir / f"stock_pivot_{ts}.csv"
    )

    pivot.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f'✅ Wrote pivot stock report → {out_csv}')

if __name__ == "__main__":
    main()
