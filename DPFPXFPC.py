================================================================================
EIBDAWSA - Average Savings Account Analysis
================================================================================
Report Date : 12/11/2026  (today - 1)
Week        : 2
Input       : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDAWSA/DPTRBLGS.parquet
Output dir  : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
Input size  : 1.74 GB
Initializing DuckDB ...
Registering parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDAWSA/DPTRBLGS.parquet
Parquet columns: BANKNO, REPTNO, FMTCODE, BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, HOLDAMT, INTPAID, ODPLAN, RATE1, RATE2, RATE3, RATE4, RATE5, TODRATE, FLATRATE, BASERATE, ODSTAT, ORGCODE, ORGTYPE, LIMIT1, LIMIT2, LIMIT3, LIMIT4, LIMIT5, INTYTD, FEEPD, PURPOSE, COL1, COL2, COL3, COL4, COL5, SECTOR, USER2, USER3, RISKCODE, LEDGBAL, DATELSTDEP, L_DEP, STATCD, DECEASED1, DECEASED2, DECEASED3, LASTTRAN, RETURNS_Y, STMT_CYCLE, CHGIND, AVGAMT, PRODUCT, RACE, BALHOLD, DEPTYPE, INT1, INTPD, INTPLAN, COMB_INDC, CURBAL, CHQFLOAT, IA_LRU, INT2, MTDLOWBA, BENINTPD, STATE, INTCYCODE, APPRLIMT, ODINTCHR, ODINTACC, CURCODE, PBIND, INTRATE, STMT_DT, YTDAVAMT, OPENIND, BDATE, INACTIVE, SECOND, ODXSAMT, BONUTYPE, SERVICE, BONUSANO, USER5, TRACKCD, EXODDATE, TEMPODDT, SCHIND, PREVBRNO, AVGBAL, COSTCTR, AUTHORISE_LIMIT, CRRCODE, CCRICODE, FAACRR, POST_IND, CENSUST, ACCPROF, MAXPROF, INTRSTPD, MTDAVBAL, VB, BILLERIND, MODIFIED_FACILITY_IND, DTLSTCUST, INTPDPYR, OPENDT, OTC_PRIN_ACCT, OPNDATEA, ACCOUNTNO, LEDBALX, BRANCHA, ACCTNOA, DATEOPNA, OPENINDA, LSTACT, CURRCODE, DORMIND, POSTIND, TBDATE, ACCTNAME, OLDIC, CLOSDATE, OPENDATE, DEPTNO, PURPCODE, SICODE, AVLBAL, SERCODE, PRODCODE, STAFFID, OFFDAT1, OFFDAT2, OFFDAT3, BMCUPDT, NEWIC, LSTWDRW, MTDACCM
Running main query ...
100% ▕██████████████████████████████████████▏ (00:00:02.51 elapsed)     
 98% ▕█████████████████████████████████████▏▏ (<1 second remaining)     Extracted 8,006 savings rows
Applying SAS formats ...
Aggregating ...
Connecting to SAS via saspy ...
SAS Connection established. Subprocess id is 4129358


21   
22   libname MISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA'  ;
NOTE: Libref MISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
23   
Writing SAS dataset MISOUT.AWSA11 ...
WARNING: expected file not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA/AWSA11.sas7bdat
SAS Connection terminated. Subprocess id was 4129358

================================================================================
SUMMARY
================================================================================
Total accounts     : 8,006
Total balance      : 26,214,065.09
Total average amt  : 33,055,595.00
New accts this year: 0

Accounts by product:
  Product 204:      3,237
  Product 207:        160
  Product 215:      4,609

Sample rows:
PURPOSE RACE CUSTCD AVGRNGE DEPRANGE  PRODUCT  NOACCT    CURBAL  AVGACCT   AVGAMT  ACCYTD  REPTDATE DENOM PRODCD
      1    1     77    1000    10000    207.0      66  12139.86       66  11126.0       0  20261112     I  42120
      1    1     77    1000     2000    204.0    1080 108253.24     1080 122970.0       0  20261112     I  42320
      1    1     77    1000     2000    215.0     137  27330.11      137  57223.0       0  20261112     I  42320
      1    1     77    1000     3000    204.0       1   2724.69        1    223.0       0  20261112     I  42320
      1    1     77    1000     3000    215.0       1   2345.84        1    689.0       0  20261112     I  42320

Completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
