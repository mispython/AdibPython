REPTDATE: 2026-08-31 10:25:22.497386
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4
TESTING MODE: Reading max 50000 rows per file

Reading CRFTABL...
Read 50000 rows from crftabl.txt (limited to 50000)
CRFT records after filter: 50000

Reading MAST file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/btmast08426.sas7bdat
Read 50000 rows from btmast08426.sas7bdat (limited to 50000)
MAST acctno dtype after conversion: Int64
CRFT acctno dtype: Int64
MAST unique acctno records: 18301
CRFT records after MAST join: 23367
CRFT final records: 23367

Reading MNITB.CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m08.sas7bdat
Read 50000 rows from intg_dp_acct_current_m08.sas7bdat (limited to 50000)
CA records: 49996

Reading MNILN.LNNOTE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat
Read 50000 rows from enrh_ln_note_m08.sas7bdat (limited to 50000)
LN records: 49964

Reading COLL and DESC files...
Warning: COLL binary file reading needs implementation for packed decimal format
File: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
Warning: DESC file reading needs implementation for fixed-width format
File: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
COLL records after merge: 0

Combining CA, LN, CRFT...
AAA total records: 123327

Merging AAA with COLL...
COLL is empty, creating empty EXCP
EXCP final records: 0

No records to write. Skipping output.


why is it no output? is it because of the filters?
