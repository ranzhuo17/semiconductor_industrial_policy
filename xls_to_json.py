#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 18 16:03:36 2025

@author: ranzhuo
"""

# scripts/xls_to_json.py
import pandas as pd
import json
import re
from pathlib import Path

EXCEL_PATH = Path("raw_data.xlsx")  # change if you use a different name
OUT_PATH = Path("policies.json")

PROJECT_SHEET = "Project Sheet"
DIST_SHEET = "Distribution Sheet"

def id_to_str(x):
    if pd.isna(x):
        return ""
    if isinstance(x, (int,)) or (isinstance(x, float) and x.is_integer()):
        return str(int(x))
    return str(x)

def clean_str(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if s in {"-", "--", "nan", "None", ""} else s

def coerce_number(x):
    if pd.isna(x):
        return None
    if isinstance(x, (int, float)):
        return int(x) if float(x).is_integer() else float(x)
    s = str(x).replace(",", "").replace("$", "").strip()
    try:
        v = float(s)
        return int(v) if v.is_integer() else v
    except:
        return str(x)  # leave as string if not numeric

def coerce_year(x):
    if pd.isna(x): 
        return ""
    s = str(x).strip()
    m = re.match(r"^(\d{4})", s)
    return m.group(1) if m else s

def main():
    xls = pd.ExcelFile(EXCEL_PATH)

    projects = pd.read_excel(EXCEL_PATH, sheet_name=PROJECT_SHEET)
    dists = pd.read_excel(EXCEL_PATH, sheet_name=DIST_SHEET)

    # Standardize columns → JSON schema
    projects = projects.rename(columns={
        "Project Name": "project_name",
        "Project ID": "project_id",
        "Subproject Name": "subproject_name",
        "Subproject ID": "subproject_id",
        "Year Announced": "year_announced",
        "Effective Period": "effective_period",
        "Country": "country_region",
        "Type & Status": "type_and_status",
        "Numbers": "numbers",
        "Targeted Firms or Parts of Value Chain": "targeted_entities",
        "Notes / Description": "notes",
        "Source": "sources",
    })

    dists = dists.rename(columns={
        "Project Name": "project_name",
        "Project ID": "project_id",
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

    # Normalize IDs
    for col in ["project_id", "subproject_id"]:
        if col in projects.columns:
            projects[col] = projects[col].map(id_to_str)
    for col in ["project_id", "distribution_id"]:
        if col in dists.columns:
            dists[col] = dists[col].map(id_to_str)

    # Clean strings
    proj_str_cols = ["project_name","subproject_name","effective_period","country_region",
                     "type_and_status","targeted_entities","notes","sources"]
    for c in proj_str_cols:
        if c in projects.columns:
            projects[c] = projects[c].map(clean_str)

    dist_str_cols = ["distribution_name","effective_period","country_region",
                     "type_and_status","targeted_entities","notes","sources"]
    for c in dist_str_cols:
        if c in dists.columns:
            dists[c] = dists[c].map(clean_str)

    # Numbers & Year
    if "numbers" in projects.columns:
        projects["numbers"] = projects["numbers"].map(coerce_number)
    if "numbers" in dists.columns:
        dists["numbers"] = dists["numbers"].map(coerce_number)

    if "year_announced" in projects.columns:
        projects["year_announced"] = projects["year_announced"].map(coerce_year)
    if "year_announced" in dists.columns:
        dists["year_announced"] = dists["year_announced"].map(coerce_year)

    # Build subproject list per project
    sub_by_pid = (projects.groupby("project_id")["subproject_name"]
                  .apply(lambda s: sorted({clean_str(x) for x in s if clean_str(x)}))
                  .to_dict())

    # Build distributions per project
    dist_fields = ["distribution_name","distribution_id","year_announced","effective_period",
                   "country_region","type_and_status","numbers","targeted_entities","notes","sources"]
    dist_by_pid = {}
    for pid, grp in dists.groupby("project_id"):
        rows = []
        for _, r in grp.iterrows():
            rows.append({k: r.get(k, "") for k in dist_fields})
        dist_by_pid[pid] = rows

    # Prefer the “main” project row (where Subproject Name is empty/"-")
    projects["is_main"] = projects["subproject_name"].map(lambda x: 1 if clean_str(x) == "" else 0)
    projects = projects.sort_values(["project_id","is_main"], ascending=[True, False]).drop_duplicates("project_id")

    out = []
    proj_fields = ["project_name","project_id","year_announced","effective_period",
                   "country_region","type_and_status","numbers","targeted_entities","notes","sources"]
    for _, r in projects.iterrows():
        pid = r["project_id"]
        entry = {k: r.get(k, "") for k in proj_fields}
        names = sub_by_pid.get(pid, [])
        entry["subproject_name"] = " / ".join(names[:3]) if names else ""
        entry["subproject_id"] = ""    # aggregated; omit or leave blank
        entry["distributions"] = dist_by_pid.get(pid, [])
        out.append(entry)

    OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {len(out)} projects to {OUT_PATH}")

if __name__ == "__main__":
    main()
