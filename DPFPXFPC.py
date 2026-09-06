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
Reading Islamic LNCOMM (ENTITY_CD = 'PIBB')...
  Islamic LNCOMM rows: 1066036
Reading Conventional LNCOMM (ENTITY_CD != 'PIBB')...
  Conventional LNCOMM rows: 1066036
Warning: INTAMT column not found. Using CORGAMT as NETPROC.
Total LOAN rows after merge: 6
Calculating ISSUED, NODAYS, ARREARS, NPLDATE...
Applying NDAYS format...
LOAN rows after deduplication: 6
Processing CISLN in chunks...
  CISLN rows after filter: 63752
Processing COLL and DESC files...

=== Examining file structures ===

=== LCCRISEX_20260831 - First 400 bytes ===
Hex dump:
  0000: 00 03 3F 00 00 00 00 13 3F C4 03 07 89 59 10 7F  ..?.....?....Y..
  0016: F2 F9 F7 F6 40 40 40 40 40 F0 F0 F8 00 00 00 04  ....@@@@@.......
  0032: 00 00 0C 09 14 19 99 25 7C D6 C2 E7 40 40 40 40  .......%|...@@@@
  0048: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0064: 40 40 40 40 40 40 F0 00 00 00 00 00 0F 00 00 00  @@@@@@..........
  0080: 00 00 0C 00 00 0F 40 40 05 0C 00 00 00 04 80 00  ......@@........
  0096: 0C 00 00 00 00 00 00 0C 00 00 00 04 80 00 0C 06  ................
  0112: 19 20 25 17 0C 00 00 00 00 00 0C 40 40 40 00 1F  . %........@@@..
  0128: 40 40 40 40 40 40 D4 E8 D9 40 40 40 40 40 40 40  @@@@@@...@@@@@@@
  0144: C4 03 07 89 59 10 7F C1 03 07 89 59 10 7F 09 14  ....Y......Y....
  0160: 19 99 25 7C 00 00 00 00 00 10 40 6C 00 00 00 00  ..%|......@l....
  0176: 00 0C 01 17 20 25 01 7C 00 00 00 00 00 10 40 6C  .... %.|......@l
  0192: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 40 00  ..............@.
  0208: 00 48 03 3F 00 00 00 00 00 23 00 00 0C F4 40 F0  .H.?.....#....@.
  0224: F9 F0 F4 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0  ................
  0240: F0 F0 F0 F0 F0 F0 F0 F0 E6 C9 D5 C7 E3 D4 40 40  ..............@@
  0256: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0272: 40 40 40 40 00 00 00 00 F2 F9 F7 F6 40 40 40 40  @@@@........@@@@
  0288: 40 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0 F0  @...............
  0304: F0 F0 F0 F0 00 00 01 9C 00 00 00 0C 00 00 00 00  ................
  0320: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
  0336: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 F1 00  ................
  0352: 00 00 00 0C 00 00 00 00 00 00 00 00 00 00 00 00  ................
  0368: 00 00 00 00 00 00 00 00 00 00 00 00 00 03 3F 00  ..............?.
  0384: 00 00 00 94 3F C4 03 07 88 48 61 5F F1 F6 F4 F3  ....?....Ha_....

=== LCCRISEX_DESC_20260831 - First 400 bytes ===
Hex dump:
  0000: F0 F0 F0 F0 F0 F0 F0 F0 F1 F3 F3 F0 F0 F8 40 00  ..............@.
  0016: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
  0032: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
  0048: 00 00 F2 F9 F7 F6 40 40 40 40 40 40 40 40 40 40  ......@@@@@@@@@@
  0064: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0080: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0096: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0112: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0128: 40 40 C4 D5 D7 40 40 40 40 40 40 40 40 40 40 40  @@...@@@@@@@@@@@
  0144: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0160: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0176: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0192: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0208: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0224: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0240: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0256: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0272: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0288: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0304: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0320: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0336: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0352: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0368: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@
  0384: 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40 40  @@@@@@@@@@@@@@@@

COLL file size: 852387880 bytes
DESC file size: 4962984400 bytes
COLL: Record length 158 divides evenly -> 5394860 records

Estimated DESC record length (based on ~58604 records): 84686

Using COLL record length: 158
Using DESC record length: 84686
Error: name 'coll_specs' is not defined
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLSMEZ.py", line 593, in <module>
    coll = read_ebcdic_fixed_records(COLL_FILE, COLL_RECORD_LENGTH, coll_specs)
NameError: name 'coll_specs' is not defined
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLSMEZ.py", line 619, in <module>
    if loan.height > 0 and coll.height > 0:
NameError: name 'coll' is not defined
