
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

PLACEHOLDERS = {"", "-", "—", "–", "`", "nan", "NaN", "None"}

def is_placeholder(x: Any) -> bool:
    if x is None:
        return True
    s = str(x).strip()
    return s in PLACEHOLDERS

def normalize_string(x: Any) -> str:
    if is_placeholder(x):
        return ""
    return str(x).strip()

def normalize_id(x: Any) -> str:
    if is_placeholder(x):
        return ""
    return str(x).strip()

def normalize_number_like(x: Any) -> str:
    if is_placeholder(x):
        return "-"
    return str(x).strip()

def load_sheets(xlsx_path: Path):
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

def prepare(proj: pd.DataFrame, dist: pd.DataFrame):
    # Normalize strings
    for col in ["project_name","subproject_name","country_region","type_and_status",
                "targeted_entities","notes","sources","effective_period"]:
        if col in proj.columns:
            proj[col] = proj[col].map(normalize_string)
        if col in dist.columns:
            dist[col] = dist[col].map(normalize_string)

    # Normalize IDs and year
    for col in ["project_id","subproject_id","year_announced"]:
        if col in proj.columns:
            if col == "year_announced":
                proj[col] = proj[col].apply(lambda v: normalize_string(v))
            else:
                proj[col] = proj[col].apply(lambda v: normalize_id(v))
        if col in dist.columns:
            if col == "year_announced":
                dist[col] = dist[col].apply(lambda v: normalize_string(v))
            else:
                dist[col] = dist[col].apply(lambda v: normalize_id(v))

    # Numbers default "-"
    if "numbers" in proj.columns:
        proj["numbers"] = proj["numbers"].map(normalize_number_like)
    if "numbers" in dist.columns:
        dist["numbers"] = dist["numbers"].map(normalize_number_like)

    # Lowercased helpers for robust name matching
    if "project_name" in proj.columns:
        proj["project_name_lc"] = proj["project_name"].str.lower()
    if "project_name" in dist.columns:
        dist["project_name_lc"] = dist["project_name"].str.lower()

    if "subproject_name" in proj.columns:
        proj["subproject_name_lc"] = proj["subproject_name"].str.lower()
    if "subproject_name" in dist.columns:
        dist["subproject_name_lc"] = dist["subproject_name"].str.lower()

    return proj, dist

def build_distributions(index_row: Dict[str, Any], dist: pd.DataFrame):
    # Strict match first
    strict = (
        (dist["project_id"].fillna("") == (index_row.get("project_id","") or "")) &
        (dist["subproject_id"].fillna("") == (index_row.get("subproject_id","") or "")) &
        (dist["subproject_name"].fillna("") == (index_row.get("subproject_name","") or "")) &
        (dist["project_name"].fillna("") == (index_row.get("project_name","") or ""))
    )
    subset = dist.loc[strict].copy()

    if subset.empty:
        # Fallback: match by names (case-insensitive) + subproject_id
        pn = (index_row.get("project_name","") or "").lower()
        spn = (index_row.get("subproject_name","") or "").lower()
        spid = (index_row.get("subproject_id","") or "")
        fallback = (
            (dist["project_name_lc"].fillna("") == pn) &
            (dist["subproject_name_lc"].fillna("") == spn) &
            (dist["subproject_id"].fillna("") == spid)
        )
        subset = dist.loc[fallback].copy()

    out = []
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

def convert(xlsx_path: Path, out_path: Path):
    proj, dist = load_sheets(xlsx_path)
    proj, dist = prepare(proj, dist)

    entries = []
    for _, row in proj.iterrows():
        entry = {
            "project_name": row.get("project_name",""),
            "project_id": normalize_id(row.get("project_id","")),
            "year_announced": normalize_string(row.get("year_announced","")),
            "effective_period": row.get("effective_period",""),
            "country_region": row.get("country_region",""),
            "type_and_status": row.get("type_and_status",""),
            "numbers": row.get("numbers","-") if str(row.get("numbers","")).strip() else "-",
            "targeted_entities": row.get("targeted_entities",""),
            "notes": row.get("notes",""),
            "sources": row.get("sources",""),  # <-- preserve Project Sheet sources
            "subproject_name": normalize_string(row.get("subproject_name","")),
            "subproject_id": normalize_id(row.get("subproject_id","")),
            "distributions": [],
        }
        entry["distributions"] = build_distributions(entry, dist)
        entries.append(entry)

    out_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2))
    return entries

def main():
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
