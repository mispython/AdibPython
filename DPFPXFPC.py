# EIID2CBT_ISLAMIC_COMBINED_BASE_DTL_REPORT  (corrected, SAS-faithful output + diagnostics)
#
# Usage:
#   python EIID2CBT.py                      # REPTDATE = yesterday (fallback, see warning)
#   python EIID2CBT.py --reptdate 2026-11-16
#   python EIID2CBT.py --no-diag            # skip diagnostics
#
# SAS original:
#   BASE  : BASE.IBTBASE&PREVMON  keep ACCTNO TRANSREF OUTSTAND (OUTSTAND -> PREOUTSTD)
#   BTDTL : BT.IBTDTL&REPTYEAR&REPTMON&REPTDAY, RETAILID R -> PRODTYPE 0, C -> 999
#   NODUPKEY on ACCTNO,TRANSREF for both, MERGE ... IF A (left join from BASE)
#   OVERDUE  = &SDATE - MATDATE,  RECOVAMT = PREOUTSTD - OUTSTAND
#   OPTIONS MISSING=0  -> any missing numeric is printed as 0

import argparse
import math
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import duckdb
import pyarrow as pa
import pyreadstat

# =====================================================================
# Configuration
# =====================================================================
SAS_EPOCH = date(1960, 1, 1)

INPUT_PATHS = {
    'ibtbase': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote',  # Islamic BASE
    'ibtdtl':  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote',  # Islamic BTDTL
}
OUTPUT_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE/EIID2CBT'

# Optional: path to the sas7bdat that holds BT.REPTDATE (column REPTDATE).
# If set it is used instead of "yesterday", exactly like the SAS job.
REPTDATE_FILE = None

# SAS ACCTNO / TRANSREF are written with the numeric format 10., so they are
# numeric in SAS. Here they arrive as strings. True = compare them as numbers
# (so '0123 ' == '123'), falling back to trimmed text when not numeric.
NUMERIC_KEYS = True

# PRODTYPE used when SUBPROD / PRODGRP cannot be mapped (None -> prints 000).
DEFAULT_PRODTYPE = 999

# SUBPROD -> PRODTYPE (only used when BTDTL has no PRODTYPE column and the row
# is not RETAILID R/C).  >>> VALUES BELOW ARE PLACEHOLDERS <<<
SUBPROD_TO_PRODTYPE = {
    'BA': 100, 'BG': 101, 'BGF': 102, 'TR': 103, 'PDB': 104, 'LC BL': 105,
    'BRULC': 106, 'SG': 107, 'FCTL': 108, 'SBL': 109, 'ECR PRE': 110,
    'LC DO': 111, 'FBEP/AP': 112, 'TBL': 113, 'ECR POST': 114, 'SBLC': 115,
    'FBEP/DOC': 116,
}

# Output record layout (1-based start col, width) -- mirrors the SAS PUT
LAYOUT = {
    'BRANCH': (1, 5), 'ACCTNO': (7, 10), 'TRANSREF': (18, 10), 'PRODTYPE': (29, 3),
    'PREOUTSTD': (33, 17), 'OUTSTAND': (51, 17), 'OVERDUE': (69, 10),
    'RECOVAMT': (80, 17), 'FACILITY': (98, 5),
}
RECLEN = 102
GAP_COLS = [6, 17, 28, 32, 50, 68, 79, 97]   # 1-based columns that must stay blank


# =====================================================================
# Helpers
# =====================================================================
def key_sql(expr):
    """SQL for a normalised join key."""
    txt = f"TRIM(CAST({expr} AS VARCHAR))"
    if NUMERIC_KEYS:
        return f"COALESCE(CAST(TRY_CAST({txt} AS DECIMAL(38,0)) AS VARCHAR), {txt})"
    return txt


def is_missing(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


def as_number(v):
    if is_missing(v):
        return None
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def sas_days(v):
    """Python date/datetime/number -> SAS day number (None if missing)."""
    if is_missing(v):
        return None
    if isinstance(v, datetime):
        return (v.date() - SAS_EPOCH).days
    if isinstance(v, date):
        return (v - SAS_EPOCH).days
    return as_number(v)


def fmt_num(v, dec):
    """SAS w.d text (unpadded). Missing -> '0' (OPTIONS MISSING=0). Rounds half up."""
    n = as_number(v)
    if n is None:
        return "0"
    q = Decimal(repr(n)).quantize(Decimal(1).scaleb(-dec), rounding=ROUND_HALF_UP)
    if q == 0:
        q = abs(q)  # avoid "-0.00"
    return format(q, "f")


def resolve_reptdate(cli_value):
    if cli_value:
        return datetime.strptime(cli_value, "%Y-%m-%d").date()
    if REPTDATE_FILE:
        df, _ = pyreadstat.read_sas7bdat(REPTDATE_FILE)
        v = df['REPTDATE'].iloc[0]
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        return SAS_EPOCH + timedelta(days=int(v))
    print("WARNING: REPTDATE not supplied; using yesterday. SAS reads BT.REPTDATE, "
          "which is NOT always yesterday (weekends/holidays/reruns). "
          "Use --reptdate or REPTDATE_FILE.")
    return date.today() - timedelta(days=1)


# =====================================================================
# Diagnostics
# =====================================================================
def probe(con, label, pairs, base_cols, dtl_cols, n_base):
    if any(b not in base_cols or d not in dtl_cols for b, d in pairs):
        print(f"  {label:<44} skipped (column missing)")
        return
    on = " AND ".join(f"{key_sql('b.' + b)} = {key_sql('d.' + d)}" for b, d in pairs)
    n = con.execute(f"SELECT COUNT(DISTINCT b.ROWIDX) FROM ibtbase b JOIN ibtdtl d ON {on}").fetchone()[0]
    print(f"  {label:<44} {n}/{n_base} base rows match")


def pre_join_diagnostics(con, df_base, df_dtl, base_dedup, btdtl_dedup):
    n_base = df_base.shape[0]
    bcols, dcols = set(df_base.columns), set(df_dtl.columns)
    print("\n================ DIAGNOSTICS: key matching ================")
    print(f"BASE rows={n_base}  BTDTL rows={df_dtl.shape[0]}")

    print("\nRaw BASE keys (repr shows hidden spaces/zeros):")
    for _, r in df_base.head(20).iterrows():
        print(f"  ACCTNO={r['ACCTNO']!r} TRANSREF={r['TRANSREF']!r} "
              f"BRANCH={r.get('BRANCH')!r} OUTSTAND={r['OUTSTAND']!r}")

    print("\nSample raw BTDTL keys:")
    for _, r in df_dtl.head(5).iterrows():
        print(f"  ACCTNO={r['ACCTNO']!r} TRANSREF={r['TRANSREF']!r}")

    exact = con.execute("""
        SELECT COUNT(DISTINCT b.ROWIDX) FROM ibtbase b JOIN ibtdtl d
        ON TRIM(CAST(b.ACCTNO AS VARCHAR)) = TRIM(CAST(d.ACCTNO AS VARCHAR))
       AND TRIM(CAST(b.TRANSREF AS VARCHAR)) = TRIM(CAST(d.TRANSREF AS VARCHAR))
    """).fetchone()[0]
    print(f"\nMatch tests (how many of the {n_base} BASE rows find a BTDTL row):")
    print(f"  {'ACCTNO+TRANSREF  exact (trimmed text)':<44} {exact}/{n_base} base rows match")
    probe(con, "ACCTNO+TRANSREF  (configured key mode)", [('ACCTNO', 'ACCTNO'), ('TRANSREF', 'TRANSREF')], bcols, dcols, n_base)
    probe(con, "ACCTNO only", [('ACCTNO', 'ACCTNO')], bcols, dcols, n_base)
    probe(con, "TRANSREF only", [('TRANSREF', 'TRANSREF')], bcols, dcols, n_base)
    probe(con, "ACCTNO+TRANSREX", [('ACCTNO', 'ACCTNO'), ('TRANSREX', 'TRANSREX')], bcols, dcols, n_base)
    probe(con, "BASE.TRANSREX = DTL.TRANSREF", [('TRANSREX', 'TRANSREF')], bcols, dcols, n_base)
    print("  Reading: ACCTNO-only high but ACCTNO+TRANSREF low => TRANSREF format differs.")
    print("           all low => accounts genuinely absent from today's BTDTL (settled/repaid).")

    dups = con.execute("""
        SELECT ACCTNO, TRANSREF, COUNT(*) c FROM (
            SELECT ACCTNO, TRANSREF FROM btdtl_trans) GROUP BY 1,2 HAVING COUNT(*)>1
    """).fetchall()
    print(f"\nBTDTL duplicate keys removed by NODUPKEY: {len(dups)} {dups[:5]}")

    print("\nRETAILID distribution in BTDTL (raw, trimmed):")
    for v, c in con.execute("SELECT RETAILID_RAW, COUNT(*) FROM btdtl_trans GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"  RETAILID={v!r} count={c}")
    print("\nPRODTYPE source (before dedup):")
    for v, c in con.execute("SELECT PRODSRC, COUNT(*) FROM btdtl_trans GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"  {v:<28} count={c}")


def post_join_diagnostics(con, rows):
    unmatched = [r for r in rows if not r['MATCHED']]
    print("\n================ DIAGNOSTICS: merge result ================")
    print(f"Combined rows={len(rows)}  matched={len(rows) - len(unmatched)}  unmatched={len(unmatched)}")
    if unmatched:
        print("Unmatched BASE keys (SAS would also print these with BRANCH/OUTSTAND/OVERDUE/RECOVAMT = 0):")
        for r in unmatched[:15]:
            cands = con.execute("SELECT ACCTNO, TRANSREF FROM btdtl_dedup WHERE ACCTNO = ? LIMIT 5",
                                [r['ACCTNO']]).fetchall()
            print(f"  ACCTNO={r['ACCTNO']!r} TRANSREF={r['TRANSREF']!r} PREOUTSTD={r['PREOUTSTD']!r} "
                  f"-> BTDTL rows with same ACCTNO: {cands}")
    print("PRODTYPE distribution after merge:")
    counts = {}
    for r in rows:
        counts[r['PRODTYPE']] = counts.get(r['PRODTYPE'], 0) + 1
    for k, v in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  PRODTYPE={k} count={v}")


def verify_output_file(path, expected_rows):
    bad = 0
    with open(path) as fh:
        lines = fh.read().split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if len(lines) != expected_rows:
        raise RuntimeError(f"Row count mismatch: file={len(lines)} expected={expected_rows}")
    for i, ln in enumerate(lines, 1):
        if len(ln) != RECLEN or any(ln[c - 1] != " " for c in GAP_COLS):
            bad += 1
            if bad <= 3:
                print(f"  LAYOUT PROBLEM line {i}: len={len(ln)} {ln!r}")
    if bad:
        raise RuntimeError(f"{bad} lines violate the SAS column layout")
    print(f"Layout check OK: {len(lines)} lines, each {RECLEN} chars, gap columns blank.")
    if lines:
        print("First record :", lines[0])
        print("Column ruler :", "".join(str((i // 10) % 10) if i % 10 == 0 else "." for i in range(1, RECLEN + 1)))


# =====================================================================
# Main
# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reptdate", help="YYYY-MM-DD (default: BT.REPTDATE file or yesterday)")
    ap.add_argument("--no-diag", action="store_true")
    args = ap.parse_args()
    diag = not args.no_diag

    # ---- Step 1: report dates -------------------------------------------------
    reptdate = resolve_reptdate(args.reptdate)
    prevdate = date(reptdate.year, reptdate.month, 1) - timedelta(days=1)
    if reptdate.month + 1 == 13:
        mm, yy = 1, reptdate.year + 1
    else:
        mm, yy = reptdate.month + 1, reptdate.year
    sdate = date(yy, mm, 1)
    sdate_sas = (sdate - SAS_EPOCH).days

    reptyear = reptdate.strftime("%y")
    reptmon = reptdate.strftime("%m")
    reptday = reptdate.strftime("%d")
    rdate = reptdate.strftime("%Y%m%d")
    prevmon = prevdate.strftime("%m")

    print(f"Report Date: {reptdate:%Y-%m-%d}")
    print(f"Previous Month: {prevmon}")
    print(f"Next Month Start: {sdate:%Y-%m-%d}")
    print(f"SDATE (SAS day number): {sdate_sas}")

    # ---- Step 2: load -----------------------------------------------------------
    base_file = os.path.join(INPUT_PATHS['ibtbase'], f"ibtbase{prevmon}.sas7bdat")
    btdtl_file = os.path.join(INPUT_PATHS['ibtdtl'], f"ibtdtl{reptyear}{reptmon}{reptday}.sas7bdat")
    for p, label in ((base_file, "Islamic BASE"), (btdtl_file, "Islamic BTDTL")):
        if not os.path.exists(p):
            raise FileNotFoundError(f"{label} file not found: {p}")

    df_base, _ = pyreadstat.read_sas7bdat(base_file)
    df_dtl, _ = pyreadstat.read_sas7bdat(btdtl_file)
    print(f"Loaded BASE : {base_file}  rows={len(df_base)}")
    print(f"Loaded BTDTL: {btdtl_file}  rows={len(df_dtl)}")

    need_base = ['ACCTNO', 'TRANSREF', 'OUTSTAND']
    need_dtl = ['BRANCH', 'ACCTNO', 'TRANSREF', 'OUTSTAND', 'MATDATE', 'FACILITY']
    miss = [c for c in need_base if c not in df_base.columns] + \
           [c for c in need_dtl if c not in df_dtl.columns]
    if miss:
        raise KeyError(f"Required columns missing: {miss}")

    # original row order = what PROC SORT NODUPKEY uses to pick "first"
    df_base["ROWIDX"] = range(len(df_base))
    df_dtl["ROWIDX"] = range(len(df_dtl))

    con = duckdb.connect()
    con.register("ibtbase", pa.Table.from_pandas(df_base, preserve_index=False))
    con.register("ibtdtl", pa.Table.from_pandas(df_dtl, preserve_index=False))
    dtl_cols = set(df_dtl.columns)

    # ---- Step 3: BASE -----------------------------------------------------------
    con.execute(f"""
        CREATE TABLE base_trans AS
        SELECT ROWIDX,
               {key_sql('ACCTNO')}   AS ACCTNO,
               {key_sql('TRANSREF')} AS TRANSREF,
               OUTSTAND AS PREOUTSTD
        FROM ibtbase
    """)

    # ---- Step 4: BTDTL (PRODTYPE) ---------------------------------------------
    default_sql = "NULL" if DEFAULT_PRODTYPE is None else str(DEFAULT_PRODTYPE)
    case_lines = "\n".join(f"            WHEN '{k}' THEN {v}" for k, v in SUBPROD_TO_PRODTYPE.items())
    if 'PRODTYPE' in dtl_cols:
        prodtype_expr = f"COALESCE(TRY_CAST(PRODTYPE AS INT), {default_sql})"
        src_label = "BTDTL.PRODTYPE column"
    elif 'SUBPROD' in dtl_cols:
        prodtype_expr = f"CASE TRIM(SUBPROD)\n{case_lines}\n            ELSE {default_sql} END"
        src_label = "SUBPROD map (PLACEHOLDER)"
    elif 'PRODGRP' in dtl_cols:
        prodtype_expr = f"COALESCE(TRY_CAST(PRODGRP AS INT), {default_sql})"
        src_label = "PRODGRP"
    else:
        prodtype_expr = default_sql
        src_label = "default"
    print(f"PRODTYPE source for non-R/C rows: {src_label}")

    if 'RETAILID' in dtl_cols:
        retail = "TRIM(CAST(RETAILID AS VARCHAR))"   # SAS compare is case-sensitive
    else:
        print("WARNING: RETAILID not in BTDTL; PRODTYPE override skipped")
        retail = "CAST(NULL AS VARCHAR)"

    con.execute(f"""
        CREATE TABLE btdtl_trans AS
        SELECT ROWIDX, BRANCH,
               {key_sql('ACCTNO')}   AS ACCTNO,
               {key_sql('TRANSREF')} AS TRANSREF,
               CASE WHEN {retail} = 'R' THEN 0
                    WHEN {retail} = 'C' THEN 999
                    ELSE {prodtype_expr} END AS PRODTYPE,
               CASE WHEN {retail} = 'R' THEN 'RETAILID=R'
                    WHEN {retail} = 'C' THEN 'RETAILID=C'
                    ELSE '{src_label}' END AS PRODSRC,
               {retail} AS RETAILID_RAW,
               OUTSTAND, MATDATE, FACILITY
        FROM ibtdtl
    """)

    # ---- Step 5: NODUPKEY (keep first in original order) ------------------------
    con.execute("""
        CREATE TABLE base_dedup AS
        SELECT ROWIDX, ACCTNO, TRANSREF, PREOUTSTD FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ACCTNO, TRANSREF ORDER BY ROWIDX) rn
            FROM base_trans) WHERE rn = 1
    """)
    con.execute("""
        CREATE TABLE btdtl_dedup AS
        SELECT ROWIDX, BRANCH, ACCTNO, TRANSREF, PRODTYPE, OUTSTAND, MATDATE, FACILITY FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ACCTNO, TRANSREF ORDER BY ROWIDX) rn
            FROM btdtl_trans) WHERE rn = 1
    """)
    n = lambda t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"BASE rows {n('base_trans')} -> {n('base_dedup')} after NODUPKEY")
    print(f"BTDTL rows {n('btdtl_trans')} -> {n('btdtl_dedup')} after NODUPKEY")

    if diag:
        pre_join_diagnostics(con, df_base, df_dtl, None, None)

    # ---- Step 6: MERGE ... IF A, output sorted by key like SAS -------------------
    if NUMERIC_KEYS:
        order = ("TRY_CAST(base.ACCTNO AS DECIMAL(38,0)), TRY_CAST(base.TRANSREF AS DECIMAL(38,0)), "
                 "base.ACCTNO, base.TRANSREF")
    else:
        order = "base.ACCTNO, base.TRANSREF"
    cur = con.execute(f"""
        SELECT b.BRANCH, base.ACCTNO, base.TRANSREF, b.PRODTYPE, base.PREOUTSTD,
               b.OUTSTAND, b.MATDATE, b.FACILITY, (b.ROWIDX IS NOT NULL) AS MATCHED
        FROM base_dedup base
        LEFT JOIN btdtl_dedup b ON base.ACCTNO = b.ACCTNO AND base.TRANSREF = b.TRANSREF
        ORDER BY {order}
    """)
    colnames = [d[0] for d in cur.description]
    rows = [dict(zip(colnames, rec)) for rec in cur.fetchall()]
    print(f"\nMerge complete. Combined rows: {len(rows)}")

    if diag:
        post_join_diagnostics(con, rows)

    # ---- Step 7: fixed-width export (SAS PUT with column pointers) ---------------
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    output_file = os.path.join(OUTPUT_PATH, f"DAYBTRD_{rdate}.txt")

    overflow = 0

    def place(buf, name, text, left=False):
        nonlocal overflow
        col, width = LAYOUT[name]
        if len(text) > width:
            overflow += 1
            text = "*" * width
        text = text.ljust(width) if left else text.rjust(width)
        buf[col - 1:col - 1 + width] = list(text)

    tot_pre = tot_out = tot_rec = 0.0
    with open(output_file, "w") as f:
        for r in rows:
            buf = [" "] * RECLEN
            pre, out = as_number(r['PREOUTSTD']), as_number(r['OUTSTAND'])
            mat = sas_days(r['MATDATE'])
            overdue = None if mat is None else sdate_sas - mat           # missing propagates
            recov = None if (pre is None or out is None) else pre - out  # missing propagates
            prod = as_number(r['PRODTYPE'])

            place(buf, 'BRANCH', fmt_num(r['BRANCH'], 0))
            place(buf, 'ACCTNO', "0" if is_missing(r['ACCTNO']) else str(r['ACCTNO']).strip())
            place(buf, 'TRANSREF', "0" if is_missing(r['TRANSREF']) else str(r['TRANSREF']).strip())
            place(buf, 'PRODTYPE', f"{int(prod):03d}" if prod is not None else "000")
            place(buf, 'PREOUTSTD', fmt_num(pre, 2))
            place(buf, 'OUTSTAND', fmt_num(out, 2))
            place(buf, 'OVERDUE', fmt_num(overdue, 0))
            place(buf, 'RECOVAMT', fmt_num(recov, 2))
            fac = "" if is_missing(r['FACILITY']) else str(r['FACILITY'])
            place(buf, 'FACILITY', fac[:5], left=True)

            f.write("".join(buf) + "\n")
            tot_pre += pre or 0.0
            tot_out += out or 0.0
            tot_rec += recov or 0.0

    print(f"\nIslamic BT Report generated: {output_file}")
    print(f"Total records written: {len(rows)}")
    if overflow:
        print(f"WARNING: {overflow} field(s) exceeded their SAS width and were written as '*'")

    print("\n================ RECONCILIATION (compare with SAS) ================")
    print(f"Records        : {len(rows)}")
    print(f"Sum PREOUTSTD  : {tot_pre:,.2f}")
    print(f"Sum OUTSTAND   : {tot_out:,.2f}")
    print(f"Sum RECOVAMT   : {tot_rec:,.2f}")
    verify_output_file(output_file, len(rows))

    con.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
