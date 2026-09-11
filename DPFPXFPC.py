Step 1: Setting report date...
Report Date: 2026-09-08 11:04:50.892976, RDATE: 24357
COLL_FILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
DESC_FILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831

Step 2: Processing credit facility table...
CRFTABL rows after parsing: 98023
Sample: [{'RECTYP1': 'P', 'BRANCH': 'BF        1', 'SUBACCT': 'SGXX', 'TFID': 'JSS/000587/06', 'ACCTNO': 2500001815}, {'RECTYP1': 'P', 'BRANCH': 'BF        1', 'SUBACCT': 'SGLX', 'TFID': 'JSS/000325/95', 'ACCTNO': 2500001912}, {'RECTYP1': 'P', 'BRANCH': 'BF        1', 'SUBACCT': 'LCB2', 'TFID': 'TDA/000011/06', 'ACCTNO': 2500003708}]

Step 3: Merging with master account data...
MAST rows: 61295
After MAST merge: 60311
CRFT_FINAL rows: 60311, CRFT1 rows: 43008

Step 4: Processing credit data...
CRED rows: 131908
After CRFT merge: 0
After filter/dedup: 0

Step 5: Summarizing credit outstanding...
CRED1 rows: 0

Step 6: Processing provision data...
PROV rows after NPLIND filter: 2513
After joining PROV+CRED: 0
CRED2 rows: 0

Step 7: Processing subaccount data...
SUBA rows: 949895
After CRFT1 merge: 307
SUBA final rows: 47, LIMTCURM summary rows: 58

Step 8: Processing collateral data (EBCDIC)...
COLL record length: None (? records)
DESC record length: 400 (12407461 records)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBTNPGS.py", line 379, in <module>
    coll_records = read_ebcdic_records(COLL_FILE, COLL_RECLEN)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBTNPGS.py", line 361, in read_ebcdic_records
    n = len(decoded) // record_len
TypeError: unsupported operand type(s) for //: 'int' and 'NoneType'
