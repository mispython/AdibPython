REPTDATE: 2026-08-31 16:08:36.835906
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4
TESTING MODE:
  - SAS7BDAT files: limited to 50000 rows
  - CRFTABL text file: limited to 50000 rows
  - COLL/DESC files: reading ALL rows

============================================================
Processing CRFTABL...
============================================================
Read 50000 rows from crftabl.txt (limited to 50000)
CRFT records after filter: 50000

Reading MAST file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/btmast08426.sas7bdat
Read 50000 rows from btmast08426.sas7bdat (limited to 50000)
MAST unique acctno records: 18301
CRFT records after MAST join: 23367
CRFT final records: 23367

============================================================
Processing MNITB.CURRENT...
============================================================
Read 50000 rows from intg_dp_acct_current_m08.sas7bdat (limited to 50000)
CA records: 49996

============================================================
Processing MNILN.LNNOTE...
============================================================
Read 50000 rows from enrh_ln_note_m08.sas7bdat (limited to 50000)
LN records: 49964

============================================================
Processing COLL and DESC files...
============================================================

Reading COLL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
File size: 0.79 GB
Detected record length: 152
Total records: 5607815
Processed 1000000 records...
Processed 2000000 records...
Processed 3000000 records...
Processed 4000000 records...
Processed 5000000 records...
Total valid COLL records: 1121563

Reading DESC file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
File size: 4.62 GB

Debugging DESC file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
File size: 4962984400 bytes

First 500 bytes (hex):
f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000f2f9f7f640404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040c4d5d740404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040404040

First 500 bytes (ASCII):
��������������@����@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@���@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

First 500 bytes (latin-1):
ððððððððñóóððø@òù÷ö@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ÄÕ×@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

Newlines in first 2000 bytes: 0
Carriage returns in first 2000 bytes: 0

Looking for patterns...
No newlines found - fixed-width format
Possible record length 200: 72.00% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 220: 74.55% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 240: 76.67% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 250: 77.60% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 256: 78.12% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 300: 81.33% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 320: 82.50% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 400: 86.00% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 500: 88.80% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 512: 89.06% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 640: 91.25% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 800: 93.00% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@
Possible record length 1000: 93.60% similarity
  Record 1 first 50 bytes: f0f0f0f0f0f0f0f0f1f3f3f0f0f8400000000000000000000000000000000000000000000000000000000000000000000000
  Record 1 ascii: ��������������@

Trying to read as line-delimited file...
Line 0: ððððððððñóóððø@òù÷ö@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ÄÕ×@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Line 0 length: 4962984400
Total lines processed: 1
Total valid DESC records: 0
COLL or DESC is empty

============================================================
Combining CA, LN, CRFT...
============================================================
AAA total records: 123327

============================================================
Merging AAA with COLL...
============================================================
COLL is empty, creating empty EXCP
EXCP final records: 0

No records to write. Skipping output.
