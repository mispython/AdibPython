EPTDATE: 2026-08-31 10:43:59.990405
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
File size: 0.75 GB
Using minimum record length: 151
Total records: 5358888
Processed 1000000 records...
Processed 2000000 records...
Processed 3000000 records...
Processed 4000000 records...
Processed 5000000 records...
Total valid COLL records: 138086

Reading DESC file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
File size: 4.42 GB
Total DESC records processed: 1
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
