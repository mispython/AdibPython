============================================================
STEP 1: Reading fixed-width file...
============================================================
File size: 185,874,284 bytes
Records: 434,285
Parsing records...
Parsing complete!

============================================================
STEP 2: Converting data types...
============================================================
Converting string columns...
String conversion complete!
Converting integer columns...
Converting decimal columns...
Conversion complete!

============================================================
STEP 3: Writing output files...
============================================================
✓ Parquet saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMADRE/ADDR_SAVINGS.parquet
✓ CSV saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMADRE/ADDR_SAVINGS.csv

REPTDATE (yesterday): 2026-08-11

============================================================
STEP 4: Validation...
============================================================
Total rows: 434,285

Sample records with valid ACCTNO:
   BANKNO APPCODE          ACCTNO      BRANCH             NAME      OLDIC        NEWIC       LEDGBAL        CURBAL        OPENDATE  PRODUCT
0    8224          35322350018592   538976288  19 JLN JENJAROM                     PUR  9.042522e+15 -2.020202e+10  35322350018592     8224
1       0            548214415392   538976288        Á*Á*Ìì  îá1PER B               2.456895e+16  2.400043e+16  93751691725139    21536
2   21024          35322350018592   538976288       MALAYSIA D        /ON  0000000000  7.160127e+15  1.384739e+16               0    20000
3    8260       R  35546244980819  1330597408             HOCK    UNIT 2-         M JL  0.000000e+00  2.002019e+16  55169679117909    21313
4    8224          35322350018592   538987841   LAYSIA D
