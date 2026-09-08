============================================================
Processing date: 2026-08-31 (SAS date: 24349)
============================================================

============================================================
STEP 1: Reading LNNOTE
============================================================
NOTE: LOANTYPE=575 not found. Checking available loan types...
Available: LOANTYPE=570 with CENSUS=0.0 (5 records)
Will use LOANTYPE=570 as fallback for testing...
Reading LNNOTE with essential columns only (using LOANTYPE=570)...
Filtered LNNOTE rows: 122
LOAN0: 116 rows, LOAN1: 6 rows

Sample LNNOTE data:
shape: (5, 15)
┌──────────┬──────────────────────────┬─────────┬──────────┬───┬─────────┬──────────┬─────────┬─────────┐
│ ACCTNO   ┆ NAME                     ┆ NOTENO  ┆ LOANTYPE ┆ … ┆ PENDBRH ┆ NETPROC  ┆ PRODUCT ┆ CENSUST │
│ ---      ┆ ---                      ┆ ---     ┆ ---      ┆   ┆ ---     ┆ ---      ┆ ---     ┆ ---     │
│ f64      ┆ str                      ┆ f64     ┆ f64      ┆   ┆ f64     ┆ f64      ┆ f64     ┆ f64     │
╞══════════╪══════════════════════════╪═════════╪══════════╪═══╪═════════╪══════════╪═════════╪═════════╡
│ 2.0000e9 ┆ TNC PLASTIC SDN BHD      ┆ 10.0    ┆ 570.0    ┆ … ┆ 2.0     ┆ 100000.0 ┆ 570.0   ┆ 0.0     │
│ 2.0001e9 ┆ HONG KEE TRADING CO      ┆ 10.0    ┆ 570.0    ┆ … ┆ 2.0     ┆ 150000.0 ┆ 570.0   ┆ 0.0     │
│ 2.0001e9 ┆ BESTFAIR PROMOTION SDN B ┆ 30010.0 ┆ 570.0    ┆ … ┆ 2.0     ┆ 335000.0 ┆ 570.0   ┆ 0.0     │
│ 2.0001e9 ┆ LISOJAYA INDUSTRY SB     ┆ 10.0    ┆ 570.0    ┆ … ┆ 2.0     ┆ 200000.0 ┆ 570.0   ┆ 0.0     │
│ 2.0001e9 ┆ PLASTOMER SDN BHD        ┆ 10.0    ┆ 570.0    ┆ … ┆ 2.0     ┆ 100000.0 ┆ 570.0   ┆ 0.0     │
└──────────┴──────────────────────────┴─────────┴──────────┴───┴─────────┴──────────┴─────────┴─────────┘

============================================================
STEP 2: Reading LNCOMM
============================================================
LNCOMM columns: ['BANKNO', 'ACCTNO', 'COMMNO', 'CCOLLTRL', 'CPRODUCT', 'CORGAMT', 'CCURAMT', 'CAVAIAMT', 'CUSEDAMT', 'CAPPDATE', 'EXPIREDT', 'REVOVLI', 'ACTIND', 'CMBRCH', 'CORIGMT', 'CSTATE', 'HSTNCDOR', 'HSTCADAD', 'CSECTOR', 'CMHSTADJ']
Columns with INT or AMT: ['CORGAMT', 'CCURAMT', 'CAVAIAMT', 'CUSEDAMT', 'UNUSEAMT']
Reading LNCOMM...
LNCOMM rows after filter: 1066036
WARNING: INTAMT column not found, using 0

============================================================
STEP 6: Reading COLL file (EBCDIC)
============================================================
File size: 852387880 bytes

Debugging COLL file - first 5 records...
Record 1: length=852387880
  Bytes 0-20: 00033f00000000133fc403078959107ff2f9f7f6
  Bytes 140-160: 40404040c403078959107fc103078959107f0914

Parsing EBCDIC COLL file...
  Total lines read: 1
  Records parsed: 0
  Parse errors: 0
WARNING: No COLL records found!

============================================================
STEP 7: Reading DESC file (EBCDIC)
============================================================
File size: 4962984400 bytes

Debugging DESC file - first 5 records...
Record 1: length=4962984400
  CCOLLNO (0-11): 00000000133
  CINSTCL (50-52): 29
  NATGUAR (54-56):   
  CGCGUR (127-130):    
  CENSUS (210-220):           
  TRANCHE (290-298):         

Parsing EBCDIC DESC file...
  Total lines read: 1
  Records parsed: 0
  Parse errors: 0
WARNING: No DESC records found!

WARNING: Cannot merge COLL and DESC - insufficient data

============================================================
Insufficient data to create NPGS
LOAN1 rows: 6
COLL rows: 0
============================================================

============================================================
Processing complete!
============================================================


probably errors on reading and parsing the ebcdic files. anything u need from my end?
