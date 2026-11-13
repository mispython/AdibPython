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
Pre-filtering records ...
100% ▕██████████████████████████████████████▏ (00:00:04.62 elapsed)     
 99% ▕█████████████████████████████████████▌▏ (<1 second remaining)     Filtered rows: 8,006
Applying SAS formats ...
Aggregating ...
Connecting to SAS via saspy ...
SAS Connection established. Subprocess id is 1238687


21   
22   libname MISOUT    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA'  ;
NOTE: Libref MISOUT was successfully assigned as follows: 
      Engine:        V9 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
23   
Writing MISOUT.AWSA11 (545 rows) ...
SAS dataset written: MISOUT.AWSA11  (0 bytes)
SAS Connection terminated. Subprocess id was 1238687

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
      1    1     77    1000    10000      207      66  12139.86       66  11126.0       0  20261112     I  42120
      1    1     77    1000     2000      204    1080 108253.24     1080 122970.0       0  20261112     I  42320
      1    1     77    1000     2000      215     137  27330.11      137  57223.0       0  20261112     I  42320
      1    1     77    1000     3000      204       1   2724.69        1    223.0       0  20261112     I  42320
      1    1     77    1000     3000      215       1   2345.84        1    689.0       0  20261112     I  42320

Completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
