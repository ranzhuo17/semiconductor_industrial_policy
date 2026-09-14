#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert raw_data.xlsx -> policies.json for the site.

Enriched output (added over the previous version):
  - `usd_m`      numeric USD in millions (from the Project sheet's `In USD` column)
  - `focus`      value-chain focus (from `Focus`)
  - `main_type`  policy type (from `Main Type`)
Each distribution now also carries:
  - `usd_m`      numeric USD in millions (from `USD (in $Ms)`)
  - `focus`      value-chain focus (from the Distribution sheet's `Main Type`)

The existing text fields (`numbers`, `type_and_status`, ...) are preserved
unchanged so the existing detail-page renderer keeps working.
"""

import argparse
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

PLACEHOLDERS = {"", "-", "\u2014", "\u2013", "`", "nan", "NaN", "None"}


def is_placeholder(x: Any) -> bool:
    if x is None:
        return True
    if isinstance(x, float) and pd.isna(x):
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


def parse_numeric(x: Any):
    """Parse `In USD` / `USD (in $Ms)` cells to a Python float in $Ms, or None."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if s in PLACEHOLDERS:
        return None
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s or s == "-" or s == ".":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_sheets(xlsx_path: Path):
    proj = pd.read_excel(xlsx_path, sheet_name="Project Sheet")
    dist = pd.read_excel(xlsx_path, sheet_name="Distribution Sheet")

    proj = proj.rename(
        columns={
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
            "In USD": "in_usd_raw",
            "Focus": "focus_raw",
            "Main Type": "main_type_raw",
        }
    )
    proj = proj.loc[:, ~proj.columns.str.startswith("Unnamed")]
    proj = proj.dropna(how="all")

    dist = dist.rename(
        columns={
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
            "USD (in $Ms)": "usd_ms_raw",
            "Main Type": "focus_raw",  # in dist sheet, Main Type carries value-chain focus
        }
    )
    dist = dist.dropna(how="all")

    return proj, dist


def prepare(proj: pd.DataFrame, dist: pd.DataFrame):
    text_cols = [
        "project_name", "subproject_name", "country_region", "type_and_status",
        "targeted_entities", "notes", "sources", "effective_period",
    ]
    for col in text_cols:
        if col in proj.columns:
            proj[col] = proj[col].map(normalize_string)
        if col in dist.columns:
            dist[col] = dist[col].map(normalize_string)

    for col in ["project_id", "subproject_id", "year_announced"]:
        if col in proj.columns:
            fn = normalize_string if col == "year_announced" else normalize_id
            proj[col] = proj[col].apply(fn)
        if col in dist.columns:
            fn = normalize_string if col == "year_announced" else normalize_id
            dist[col] = dist[col].apply(fn)

    if "distribution_name" in dist.columns:
        dist["distribution_name"] = dist["distribution_name"].map(normalize_string)
    if "distribution_id" in dist.columns:
        dist["distribution_id"] = dist["distribution_id"].apply(normalize_id)

    if "numbers" in proj.columns:
        proj["numbers"] = proj["numbers"].map(normalize_number_like)
    if "numbers" in dist.columns:
        dist["numbers"] = dist["numbers"].map(normalize_number_like)

    # NEW: enriched fields for the graph builder
    proj["usd_m"] = proj["in_usd_raw"].apply(parse_numeric) if "in_usd_raw" in proj.columns else None
    proj["focus"] = proj["focus_raw"].map(lambda v: re.sub(r"\s+", " ", str(v)).strip()
                                          if not is_placeholder(v) else "") if "focus_raw" in proj.columns else ""
    proj["main_type"] = proj["main_type_raw"].map(normalize_string) if "main_type_raw" in proj.columns else ""

    dist["usd_m"] = dist["usd_ms_raw"].apply(parse_numeric) if "usd_ms_raw" in dist.columns else None
    dist["focus"] = dist["focus_raw"].map(lambda v: re.sub(r"\s+", " ", str(v)).strip()
                                          if not is_placeholder(v) else "") if "focus_raw" in dist.columns else ""

    for df in (proj, dist):
        if "project_name" in df.columns:
            df["project_name_lc"] = df["project_name"].str.lower()
        if "subproject_name" in df.columns:
            df["subproject_name_lc"] = df["subproject_name"].str.lower()

    return proj, dist



def attach_distributions_globally(entries: list, dist: pd.DataFrame):
    """Attach each distribution to the project entry with the same
    (project_id, subproject_id). Falls back to project_id alone when the
    subproject field disagrees. Prints a report of rows that could not be
    matched so they can be fixed in the source workbook."""
    by_pid_sub, by_pid = {}, {}
    for idx, e in enumerate(entries):
        pid = (e.get("project_id") or "").strip()
        sub = (e.get("subproject_id") or "").strip()
        by_pid_sub.setdefault((pid, sub), idx)
        by_pid.setdefault(pid, idx)
        e["distributions"] = []

    proj_names = {(e.get("project_id") or "").strip(): e.get("project_name", "") for e in entries}
    orphans, name_mismatch = [], []
    for _, drow in dist.iterrows():
        dname = str(drow.get("distribution_name", "") or "").strip()
        pid = str(drow.get("project_id", "") or "").strip()
        sub = str(drow.get("subproject_id", "") or "").strip()
        target = by_pid_sub.get((pid, sub))
        if target is None:
            target = by_pid.get(pid)
        if target is None:
            orphans.append((pid, str(drow.get("project_name", "")), dname))
            continue
        pn_dist = str(drow.get("project_name", "") or "").strip().lower()
        pn_proj = proj_names.get(pid, "").strip().lower()
        if pn_dist and pn_proj and pn_dist != pn_proj:
            name_mismatch.append((pid, drow.get("project_name", ""), proj_names.get(pid, ""), dname))
        entries[target]["distributions"].append({
            "distribution_name": dname,
            "distribution_id": drow.get("distribution_id", ""),
            "year_announced": drow.get("year_announced", ""),
            "effective_period": drow.get("effective_period", ""),
            "country_region": drow.get("country_region", ""),
            "type_and_status": drow.get("type_and_status", ""),
            "numbers": drow.get("numbers", "-") if str(drow.get("numbers", "")).strip() else "-",
            "targeted_entities": drow.get("targeted_entities", ""),
            "notes": drow.get("notes", ""),
            "sources": drow.get("sources", ""),
            "usd_m": drow.get("usd_m"),
            "focus": drow.get("focus", ""),
        })

    if orphans or name_mismatch:
        print("\nData-quality report (fix these in raw_data.xlsx):")
        seen = set()
        for pid, pn, dn in orphans:
            key = (pid, pn)
            if key in seen:
                continue
            seen.add(key)
            n = sum(1 for o in orphans if (o[0], o[1]) == key)
            print(f"  ORPHAN    Project ID {pid} '{pn}' has no Project-sheet row ({n} distributions dropped)")
        seen = set()
        for pid, pn_d, pn_p, dn in name_mismatch:
            key = (pid, pn_d)
            if key in seen:
                continue
            seen.add(key)
            print(f"  MISMATCH  Project ID {pid}: distribution says '{pn_d}', Project sheet says '{pn_p}'")


def convert(xlsx_path: Path, out_path: Path):
    proj, dist = load_sheets(xlsx_path)
    proj, dist = prepare(proj, dist)

    def _num(v):
        """None if missing/NaN, otherwise a plain float."""
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        try:
            f = float(v)
            if f != f:  # NaN
                return None
            return f
        except (TypeError, ValueError):
            return None

    entries = []
    for _, row in proj.iterrows():
        entry = {
            "project_name": row.get("project_name", ""),
            "project_id": normalize_id(row.get("project_id", "")),
            "year_announced": normalize_string(row.get("year_announced", "")),
            "effective_period": row.get("effective_period", ""),
            "country_region": row.get("country_region", ""),
            "type_and_status": row.get("type_and_status", ""),
            "numbers": row.get("numbers", "-") if str(row.get("numbers", "")).strip() else "-",
            "targeted_entities": row.get("targeted_entities", ""),
            "notes": row.get("notes", ""),
            "sources": row.get("sources", ""),
            "subproject_name": normalize_string(row.get("subproject_name", "")),
            "subproject_id": normalize_id(row.get("subproject_id", "")),
            "usd_m": _num(row.get("usd_m")),
            "focus": row.get("focus", ""),
            "main_type": row.get("main_type", ""),
            "distributions": [],
        }
        entries.append(entry)

    # Attach distributions globally so each dist row goes to exactly one project.
    attach_distributions_globally(entries, dist)

    # Scrub NaN out of distribution usd_m values (pandas -> Python)
    for e in entries:
        for d in e["distributions"]:
            d["usd_m"] = _num(d.get("usd_m"))

    # ensure_ascii=False for readability; allow_nan=False to force real JSON
    out_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2, allow_nan=False))
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
