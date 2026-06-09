Skip to content
mispython
AdibPython
Repository navigation
Code
Issues
Pull requests
Agents
Actions
Projects
Security and quality
Insights
Settings
Files
Go to file
t
T
DPFPXFPC.py
EIBDBKTR
EIBDBKTR.py
EIBDBTEX
EIBDBTEX.py
EIBDDCIA
EIBDDCIA.py
EIBDFDHQ
EIBDFDHQ.py
EIBDWALK
EIBDWALK.py
EIBMCCR8
EIBMCCR8.py
EIBMFEEX
EIBMFEEX.py
EIBMSTAF
EIBMSTAF.py
EIBWBTEX
EIBWBTEX.py
EIBWBTMS
EIBWBTMS.py
EIBWBTRD
EIBWBTRD.py
EIBWCC5L
EIBWCC5L.py
EIBWCCR6
EIBWCCR6.py
EIBWCCR7
EIBWCCR7.py
EIIWBTCR
EIIWBTCR.py
EIIWCC5C
EIIWCC5C.py
EIIWCC5L
EIIWCC5L.py
EIIWCCR4
EIIWCCR4.py
EIIWCCR5
EIIWCCR5.py
EIIWCCR6
EIIWCCR6.py
EIVMSTAF
EIVMSTAF.py
FTPBTWHS
Python-THORIQ.zip
activity list
activitylist 1
excel
AdibPython
/
DPFPXFPC.py
in
main

Edit

Preview
Indent mode

Spaces
Indent size

2
Line wrap mode

No wrap
Editing DPFPXFPC.py file contents
  1
  2
  3
  4
  5
  6
  7
  8
  9
 10
 11
 12
 13
 14
 15
 16
 17
 18
 19
 20
 21
 22
 23
 24
 25
 26
 27
 28
 29
 30
 31
 32
 33
 34
 35
 36
from __future__ import annotations
from pathlib import Path
from datetime import date, timedelta
import polars as pl
import duckdb  # as requested
import pyarrow.parquet as pq  # as requested

# ---------- SAS-like libs (adjust paths only) ----------
DPAA_TXT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/uat/RBP2.B033.ODPA.EXT.FILE.MIS.txt")  # Input text file

# Output directory
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- 1) DATA LMTDET.LMTDET ----------
# Read directly from the txt file
req = [
 "AANO","APRVDT","APRVAMT","ACCTNO","TOTLMTAMT","LASTMNTDT","LMTID","LMTAMT",
 "LMTSTARTDT","LMTENDDT","LMTTERM","LMTTERMID","LMTPAIDIND",
 "COLL1","COLL2","COLL3","COLL4","COLL5","COLL6","COLL7","COLL8","COLL9","COLL10"
]

# Check if text file exists
if not DPAA_TXT.exists():
    raise FileNotFoundError(f"Text file not found: {DPAA_TXT}")

print(f"Reading from text file: {DPAA_TXT}")

# Try different delimiters
delimiters_to_try = ['\t', '|', ',', ' ']
df = None

for delimiter in delimiters_to_try:
    try:
        print(f"  Trying delimiter: '{delimiter}'")
        df = pl.read_csv(
Use Control + Shift + m to toggle the tab key moving focus. Alternatively, use esc then tab to move to the next interactive element on the page.
