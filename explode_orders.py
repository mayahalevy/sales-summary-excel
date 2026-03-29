
"""
Explode orders with aggregated products into one row per product.

Usage:
    python explode_orders.py input.csv output.csv --product-col "Products"

If --product-col is omitted, the script will guess.
"""
import argparse
import json
import re
import pandas as pd
from pathlib import Path

def detect_products_column(columns):
    lowered = [c.lower() for c in columns]
    scores = {}
    for col, low in zip(columns, lowered):
        score = 0
        if any(tok in low for tok in ["product", "item", "line"]):
            score += 2
        if any(tok in low for tok in ["products", "items", "line_items"]):
            score += 2
        if any(tok in low for tok in ["_id", "id", "sku"]) and not re.search(r"(names?|titles?|descriptions?)", low):
            score -= 2
        if re.search(r"(name|title|description)s?", low):
            score += 1
        scores[col] = score
    return max(scores, key=lambda k: scores[k])

def parse_products_cell(val):
    if pd.isna(val):
        return []
    if isinstance(val, list):
        return val
    s = str(val).strip()
    if not s:
        return []
    if s.startswith("[") and s.endswith("]"):
        try:
            data = json.loads(s)
            if isinstance(data, list):
                out = []
                for x in data:
                    if isinstance(x, dict):
                        name = x.get("name") or x.get("title") or x.get("description") or x.get("product") or x.get("item")
                        if name:
                            parts = [name]
                            if x.get("quantity") is not None:
                                parts.append(f"qty={x['quantity']}")
                            if x.get("qty") is not None:
                                parts.append(f"qty={x['qty']}")
                            if x.get("sku"):
                                parts.append(f"sku={x['sku']}")
                            out.append(" | ".join(parts))
                        else:
                            out.append(json.dumps(x, ensure_ascii=False))
                    else:
                        out.append(str(x))
                return out
        except Exception:
            pass
    if s.startswith("{") and s.endswith("}"):
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                for key in ["items", "products", "line_items", "lines"]:
                    if key in obj and isinstance(obj[key], list):
                        return parse_products_cell(json.dumps(obj[key], ensure_ascii=False))
        except Exception:
            pass
    seps = ["|", ";", "\n", " • ", " · ", " / ", " + ", " •", " ·", " /", " +", "¦"]
    counts = {sep: s.count(sep) for sep in seps}
    best_sep = max(counts, key=counts.get)
    if counts[best_sep] > 0:
        return [p.strip() for p in s.split(best_sep) if p.strip()]
    if s.count(",") >= 2:
        parts = [p.strip() for p in re.split(r",(?![^(]*\))", s) if p.strip()]
        return parts
    return [s]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=str)
    parser.add_argument("output_csv", type=str)
    parser.add_argument("--product-col", type=str, default=None)
    args = parser.parse_args()

    df = pd.read_csv(args.input_csv)
    prod_col = args.product_col or detect_products_column(df.columns)

    df["__items_list"] = df[prod_col].apply(parse_products_cell)
    out = df.explode("__items_list", ignore_index=True).rename(columns={"__items_list": "item"})
    out.to_csv(args.output_csv, index=False)
    print(f"Wrote {args.output_csv} with {len(out):,} rows. Product column used: {prod_col!r}")

if __name__ == "__main__":
    main()

