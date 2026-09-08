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
COLL record length: 158
DESC record length: 84686

=== Scanning DESC file for non-empty records ===

Record 0 (0-based index):
  First 200 chars: [00000000133008 2976                                                                            DNP                                                                   ]
  CCOLLNO: '00000000133'
  Pos 51-52: '29'
  Pos 55-56: '  '

Record 2 (0-based index):
  First 200 chars: [                            00000000                                                                                                                                                                    ]
  CCOLLNO: ''
  Pos 51-52: '  '
  Pos 55-56: '  '

Record 13 (0-based index):
  First 200 chars: [                                                                                                                                    00000095547008 7552              ]
  CCOLLNO: ''
  Pos 51-52: '  '
  Pos 55-56: '  '

Record 15 (0-based index):
  First 200 chars: [ IC1018 REGROUPING AREA                    31450 MENGLEMBU                         BLEE SIEW LENG                           740603085482                            31450 MENGLEMBU PERAK               ]
  '18' found at positions: [6, 289]
  '06' found at positions: [127]
  CCOLLNO: 'IC1018 REG'
  Pos 51-52: 'EN'
  Pos 55-56: 'EM'

Record 16 (0-based index):
  First 200 chars: [                    31400  Y      /Ç
                                                        MY      
                                                                I       0000000029112024N30112022                       21                                      TINGKAT TAMAN IPOH 9                    TAMAN ]
  CCOLLNO: ''
  Pos 51-52: ''
  Pos 55-56: ''

Total non-empty records found in first 10000 records: 5791

=== Checking if DESC is line-delimited ===
No newline characters found in first 1000 bytes

=== Reading DESC as line-delimited ===
Line 0: [00000000133008 2976                                                                            DNP                                                                   ]
Line 1: [
          FN                                                                                                          80100 C       
                                                                                                                                    MY      &
                                                                                                                                                     0000000027112025G04042023                ]
Line 2: [
         MY      
                 I       0000000031052016C15122015                       17                                      JALAN KESUMA 2/1                        BANDAR TASIK KESUMA                     SEMEN]
Line 3: [
         MY      
]
Line 4: [
         I       0000000012072023C14012022                       23                                                                              NEW WORLD COMMERCIAL CENTRE, D                               ]
Line 5: [
          FN                                        TAMAN KIM LAM - LOT 16                                            88300         
                                                                                                                                    MY      
                                                                                                                                            C       0000000029112024N30112022]
Line 6: [
                   PATRICIA LIAW NYUK LIN                  680315135148                                                                    WIFE                                                         ]
Line 7: [
                 2014          GOH AH SOON @ GOH BOON SIM              550510135076                            TAN HUI CHOO                            560420135530                                            ]
Line 8: [
                 2012          YAP CHEE SEN                            680301086429                                                                                                                           ]
Line 9: [
                   WEE KOK SIANG                           661210135607                                                                                                                                 ]

=== End Enhanced Diagnostic ===

Reading COLL file...
COLL rows: 5394860
Reading DESC file...
DESC rows: 58604

COLL rows after join: 593672197
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
Processing MICR file...
  MICR rows: 300
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 419047

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 419047
