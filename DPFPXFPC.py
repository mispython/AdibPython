REPTDATE: 2026-08-31 10:33:41.995165
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4
TESTING MODE: Reading max 50000 rows per file

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
File exists: True
File size: 809192140 bytes
Text ratio: 29.20%
File appears to be BINARY format
Total bytes: 809192140
Using minimum record length: 151
Record 0: CCOLLNO bytes=00000000133f, ACCTNO bytes=03078959107f
  CCOLLNO=133, ACCTNO=3078959107
Error reading COLL file: invalid literal for int() with base 10: '8959107f091'

Reading DESC file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
File exists: True
File size: 4750088300 bytes
Text ratio: 88.80%
File appears to be TEXT format
Error reading DESC file: 'utf-8' codec can't decode byte 0xf0 in position 0: invalid continuation byte
COLL records after merge: 0 (empty input)

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
