Report Date: 2026-08-31
Normalization Date: 31/08/2026
Reading LOAN/LNNOTE datasets in chunks...
Reading Islamic LNNOTE (ENTITY_CD = 'PIBB')...
  Islamic LNNOTE rows: 6
Reading Conventional LNNOTE (ENTITY_CD != 'PIBB')...
  Conventional LNNOTE rows: 99994
Combining LNNOTE datasets...
  LOAN0 rows: 6
  LOAN1 rows: 0
Reading COMM datasets in chunks...
Reading Islamic LNCOMM...
  Islamic LNCOMM rows: 1066036
Reading Conventional LNCOMM...
  Conventional LNCOMM rows: 1066036
Warning: INTAMT column not found. Using CORGAMT as NETPROC.
Total LOAN rows after merge: 6
Calculating ISSUED, NODAYS, ARREARS, NPLDATE...
Applying NDAYS format...
LOAN rows after deduplication: 6
Processing CISLN in chunks...
  CISLN rows after filter: 63752
Processing COLL and DESC files...

Reading COLL file...
COLL rows: 5394860

=== Scanning DESC for NATGUAR='06' position ===

Lines with '06' in first 300 chars: 12
  Line 7: '06' at pos 298
    Context: ...    21122006608 M        ...
    CCOLLNO: '
                      2'
    Pos 51-52: '  '
    Pos 55-56: '  '
  Line 15: '06' at pos 36
    Context: ...  0000000006052016P270220...
    CCOLLNO: '-
               MY'
    Pos 51-52: '16'
    Pos 55-56: '  '
  Line 16: '06' at pos 80
    Context: ...     B1-2-06   ANGGERIK V...
    CCOLLNO: '
              MY      '
    Pos 51-52: '3 '
    Pos 55-56: '  '

Lines with '18' in first 300 chars: 3
  Line 33: '18' at pos 176
    Context: ...031102014L18121996       ...
    CCOLLNO: '
                NMEDAN'
    Pos 51-52: 'BU'
    Pos 55-56: 'A '
  Line 45: '18' at pos 69
    Context: ...      680318085350       ...
    CCOLLNO: '&%        2'
    Pos 51-52: '  '
    Pos 55-56: '  '
  Line 46: '18' at pos 42
    Context: ...000030102018L31122009...
    CCOLLNO: '
              MY'
    Pos 51-52: '09'
    Pos 55-56: ''

=== End Diagnostic ===

Reading DESC file as line-delimited text...
DESC rows: 58608

Unique CINSTCL values (first 20): ['BE', '9P', '2I', '22', '4T', '@C', 'CR', 'T8', 'OK', 'BL', 'PS', 'AC', 'EQ', '6/', '3M', 'AG', 'VA', 'RQ', '09', 'PO']
Unique NATGUAR values (first 20): ['DG', ',A', 'HN', 'GG', ')', 'E,', 'TA', '+,', 'GO', '@A', 'NG', '01', 'PI', '2', 'OL', "'", 'YL', '00', 'SH', 'YA']
Rows with CINSTCL='18': 84
Rows with NATGUAR='06': 0

COLL rows after join: 3293031
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
Processing MICR file...
  MICR rows: 300
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 424012

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 424012
