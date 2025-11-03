
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust Excel ➜ JSON converter for the policy tracker.

Usage:
  python xls_to_json_fixed.py --in raw_data.xlsx --out policies.json

Behavior:
- Reads two sheets: "Project Sheet" and "Distribution Sheet".
- Produces ONE JSON entry PER ROW of "Project Sheet" (no deduping).
- Each entry has the fields:
    {
      "project_name": ,
      "project_id": ,
      "year_announced": ,
      "effective_period": ,
      "country_region": ,
      "type_and_status": ,
      "numbers": "-",
      "targeted_entities": ,
      "notes": ,
      "subproject_name": "",
      "subproject_id": "",
      "distributions": []
    }
- Distributions from "Distribution Sheet" are placed under the correct entry
  by matching the quadruple (project_name, project_id, subproject_name, subproject_id).
- Missing/placeholder values are normalized (e.g., "-", backticks) to empty strings;
  numbers default to "-".

Author: ChatGPT (for Ran)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def normalize_string(x: Any) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s in {"-", "—", "–", "`", "nan", "NaN", "None"}:
        return ""
    return s


def normalize_number_like(x: Any) -> str:
    if pd.isna(x) or str(x).strip() in {"", "-", "—", "–", "`", "nan", "NaN", "None"}:
        return "-"
    return str(x).strip()


def load_sheets(xlsx_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    proj = pd.read_excel(xlsx_path, sheet_name="Project Sheet")
    dist = pd.read_excel(xlsx_path, sheet_name="Distribution Sheet")

    proj = proj.rename(columns={
        "Project Name": "project_name",
        "Project ID": "project_id",
        "Subproject Name": "subproject_name",
        "Subproject ID": "subproject_id",
        "Year Announced": "year_announced",
        "Effective Period": "effective_period",
        "Country / Region": "country_region",
        "Type & Status": "type_and_status",
        "Numbers": "numbers",
        "Targeted Firms or Parts of Value Chain": "targeted_entities",
        "Notes / Description": "notes",
        "Source": "sources",
    })
    proj = proj.loc[:, ~proj.columns.str.startswith("Unnamed")]
    proj = proj.dropna(how="all")

    dist = dist.rename(columns={
        "Project Name": "project_name",
        "Project ID": "project_id",
        "Subproject Name": "subproject_name",
        "Subproject ID": "subproject_id",
        "Distribution Name": "distribution_name",
        "Distribution ID": "distribution_id",
        "Year Announced": "year_announced",
        "Effective Period": "effective_period",
        "Country": "country_region",
        "Type & Status": "type_and_status",
        "Numbers": "numbers",
        "Targeted Firms or Parts of Value Chain": "targeted_entities",
        "Notes / Description": "notes",
        "Source": "sources",
    })
    dist = dist.dropna(how="all")
    return proj, dist


def prepare(proj: pd.DataFrame, dist: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Canonicalize strings
    for col in ["project_name","subproject_name","country_region","type_and_status",
                "targeted_entities","notes","sources","effective_period"]:
        if col in proj.columns:
            proj[col] = proj[col].map(normalize_string)
        if col in dist.columns:
            dist[col] = dist[col].map(normalize_string)

    # ID-ish fields and years as strings
    for col in ["project_id","subproject_id","year_announced"]:
        if col in proj.columns:
            proj[col] = proj[col].apply(lambda v: "" if pd.isna(v) else str(v).strip())
        if col in dist.columns:
            dist[col] = dist[col].apply(lambda v: "" if pd.isna(v) else str(v).strip())

    # Numbers default "-"
    if "numbers" in proj.columns:
        proj["numbers"] = proj["numbers"].map(normalize_number_like)
    if "numbers" in dist.columns:
        dist["numbers"] = dist["numbers"].map(normalize_number_like)

    return proj, dist


def build_distributions(index_row: Dict[str, Any], dist: pd.DataFrame) -> List[Dict[str, Any]]:
    mask = (
        (dist["project_name"].fillna("") == index_row.get("project_name","")) &
        (dist["project_id"].fillna("") == index_row.get("project_id","")) &
        (dist["subproject_name"].fillna("") == index_row.get("subproject_name","")) &
        (dist["subproject_id"].fillna("") == index_row.get("subproject_id",""))
    )
    subset = dist.loc[mask].copy()
    out: List[Dict[str, Any]] = []
    for _, drow in subset.iterrows():
        out.append({
            "distribution_name": drow.get("distribution_name",""),
            "distribution_id": drow.get("distribution_id",""),
            "year_announced": drow.get("year_announced",""),
            "effective_period": drow.get("effective_period",""),
            "country_region": drow.get("country_region",""),
            "type_and_status": drow.get("type_and_status",""),
            "numbers": drow.get("numbers","-") if str(drow.get("numbers","")).strip() else "-",
            "targeted_entities": drow.get("targeted_entities",""),
            "notes": drow.get("notes",""),
            "sources": drow.get("sources",""),
        })
    return out


def convert(xlsx_path: Path, out_path: Path) -> List[Dict[str, Any]]:
    proj, dist = load_sheets(xlsx_path)
    proj, dist = prepare(proj, dist)

    entries: List[Dict[str, Any]] = []
    for _, row in proj.iterrows():
        entry = {
            "project_name": row.get("project_name",""),
            "project_id": row.get("project_id",""),
            "year_announced": row.get("year_announced",""),
            "effective_period": row.get("effective_period",""),
            "country_region": row.get("country_region",""),
            "type_and_status": row.get("type_and_status",""),
            "numbers": row.get("numbers","-") if str(row.get("numbers","")).strip() else "-",
            "targeted_entities": row.get("targeted_entities",""),
            "notes": row.get("notes",""),
            "subproject_name": row.get("subproject_name","") if str(row.get("subproject_name","")).strip() not in {"-","`"} else "",
            "subproject_id": row.get("subproject_id","") if str(row.get("subproject_id","")).strip() not in {"-","`"} else "",
            "distributions": [],
        }
        entry["distributions"] = build_distributions(entry, dist)
        entries.append(entry)

    out_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2))
    return entries


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="infile", required=False, default="raw_data.xlsx")
    ap.add_argument("--out", dest="outfile", required=False, default="policies.json")
    args = ap.parse_args()

    xlsx_path = Path(args.infile)
    out_path = Path(args.outfile)
    entries = convert(xlsx_path, out_path)
    print(f"Wrote {len(entries)} entries to {out_path}")


if __name__ == "__main__":
    main()
