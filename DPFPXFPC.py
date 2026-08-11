============================================================
STEP 1: Reading EBCDIC fixed-width file...
============================================================
File size: 185,874,284 bytes
Record length: 428 bytes
Number of records: 434,285
Parsing fixed-width records...
Saving raw bytes to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMADRE/DPADDR_TEMP.parquet...
Step 1 complete! Raw data saved to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMADRE/DPADDR_TEMP.parquet

============================================================
STEP 2: Converting EBCDIC and Packed Decimal...
============================================================
Converting EBCDIC string columns...
Converting packed decimal columns (integers)...
Converting packed decimal columns (decimals)...
Conversion complete!

============================================================
STEP 3: Writing output files...
============================================================
✓ Parquet file saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMADRE/ADDR_SAVINGS.parquet
✓ CSV file saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMADRE/ADDR_SAVINGS.csv

============================================================
STEP 4: Validating output...
============================================================
Total rows: 434,285
REPTDATE (yesterday): 2026-08-10

Sample data (first 5 rows):
   BANKNO APPCODE       ACCTNO  BRANCH              NAME_PART  LEDGBAL        CURBAL  YTDBAL
0       0       à            0       0   è + çäç||      0.0  0.000000e+00     0.0
1       0       |            0       0         0.0  0.000000e+00     0.0
2       0        -20202020204       0  ëñ \ràc !      0.0  0.000000e+00     0.0
3       0                   0       0   ¢<+¢á+¢ ê|(      0.0 -2.020202e+10     0.0
4       0                   0       0   cAcA      0.0  0.000000e+00     0.0

Column data types:
   column_name column_type null   key default extra
0       BANKNO      BIGINT  YES  None    None  None
1      APPCODE     VARCHAR  YES  None    None  None
2       ACCTNO      BIGINT  YES  None    None  None
3       BRANCH      BIGINT  YES  None    None  None
4         NAME     VARCHAR  YES  None    None  None
5        OLDIC     VARCHAR  YES  None    None  None
6     OPENDATE      BIGINT  YES  None    None  None
7      PRODUCT      BIGINT  YES  None    None  None
8      OPENIND     VARCHAR  YES  None    None  None
9      PURPOSE     VARCHAR  YES  None    None  None
10        RACE     VARCHAR  YES  None    None  None
11       USER3     VARCHAR  YES  None    None  None
12     DORMANT     VARCHAR  YES  None    None  None
13     DEPTYPE     VARCHAR  YES  None    None  None
14       BDATE      BIGINT  YES  None    None  None
15      DEPTNO      BIGINT  YES  None    None  None
16       NEWIC     VARCHAR  YES  None    None  None
17     LEDGBAL      DOUBLE  YES  None    None  None
18      CURBAL      DOUBLE  YES  None    None  None
19      YTDBAL      DOUBLE  YES  None    None  None
20     YTDDAYS      BIGINT  YES  None    None  None
21    NAMETYPE     VARCHAR  YES  None    None  None
22     NAMELN1     VARCHAR  YES  None    None  None
23     NAMELN2     VARCHAR  YES  None    None  None
24     NAMELN3     VARCHAR  YES  None    None  None
25     NAMELN4     VARCHAR  YES  None    None  None
26     NAMELN5     VARCHAR  YES  None    None  None
27     NAMELN6     VARCHAR  YES  None    None  None
28     NAMELN7     VARCHAR  YES  None    None  None
29     NAMELN8     VARCHAR  YES  None    None  None

============================================================
PROGRAM COMPLETED SUCCESSFULLY!
============================================================
