#!/usr/bin/env python3
import sys
sys.path.insert(0, "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
import EIBMHPFS
from datetime import date

rv = EIBMHPFS.get_report_vars()
print(f"SDATE = {rv['sdate']}")

# Build HPSETTLE (this takes a while — 8M rows)
hpsettle = EIBMHPFS.build_hpsettle(rv)
print(f"HPSETTLE rows: {len(hpsettle)}")

# Sample 20 rows — check issdte / closedte
sample = hpsettle.head(20)
print("\nFirst 20 rows:")
print(sample.select(['acctno', 'issdte', 'closedte']))

# How many have issdte >= SDATE?
sdate = rv['sdate']
n_with_issdte = sum(
    1 for r in hpsettle.iter_rows(named=True)
    if r['issdte'] is not None and r['issdte'] >= sdate
)
n_with_closedte = sum(
    1 for r in hpsettle.iter_rows(named=True)
    if r['closedte'] is not None and r['closedte'] >= sdate
)
n_both = sum(
    1 for r in hpsettle.iter_rows(named=True)
    if r['issdte'] is not None and r['issdte'] >= sdate
    and r['closedte'] is not None and r['closedte'] >= sdate
)

print(f"\nRow counts:")
print(f"  issdte >= {sdate}:           {n_with_issdte:,}")
print(f"  closedte >= {sdate}:          {n_with_closedte:,}")
print(f"  both issdte AND closedte >=:  {n_both:,}")

# If n_both == 0, HPSNR=0 is data-correct, not a bug.
