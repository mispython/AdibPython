============================================================
STEP 0: Detecting EBCDIC encoding...
============================================================
Raw hex of NAME field:
54414e2041482043484f4f20202020202020202020202020

cp037: 'è + çäç||' (printable ratio: 37.50%)
cp1140: 'è + çäç||' (printable ratio: 37.50%)
cp273: 'è + ç{ç!!' (printable ratio: 37.50%)
cp500: 'è + çäç!!' (printable ratio: 37.50%)
cp424: 'לא+אחגח||' (printable ratio: 37.50%)
cp875: 'ΝΑ+ΑΘΓΘ!!' (printable ratio: 37.50%)
cp1026: 'è + {ä{!!' (printable ratio: 37.50%)

✓ Selected encoding: cp037

============================================================
STEP 2: Reading EBCDIC fixed-width file...
============================================================
File size: 185,874,284 bytes
Record length: 428 bytes
Number of records: 434,285
Parsing fixed-width records...
Saving raw bytes to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMADRE/DPADDR_TEMP.parquet...
Step 2 complete!

============================================================
STEP 3: Converting using encoding 'cp037'...
============================================================
Converting EBCDIC string columns...
Converting packed decimal columns (integers)...
Converting packed decimal columns (decimals)...
Conversion complete!

============================================================
STEP 4: Writing output files...
============================================================
✓ Parquet file saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMADRE/ADDR_SAVINGS.parquet
✓ CSV file saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMADRE/ADDR_SAVINGS.csv

============================================================
STEP 5: Validating output...
============================================================
Total rows: 434,285
REPTDATE (yesterday): 2026-08-10
Using encoding: cp037

Sample data (first 10 rows, key fields):
   BANKNO APPCODE       ACCTNO  BRANCH                       NAME        OLDIC         NEWIC  LEDGBAL        CURBAL  OPENDATE NAMETYPE
0       0       à            0       0   è + çäç||    
1       0       |            0       0     ¢<+è(   (íèñ      0.0  0.000000e+00         0        
2       0        -20202020204       0  ëñ \ràc !( +  à îñàë|+  u°ä      0.0  0.000000e+00         0         
3       0                   0       0   ¢<+¢á+¢ ê|(    &íê      0.0 -2.020202e+10         0        
4       0                   0       0   cAcAö"Ö  Ó÷
                                                   a&áêâ        0.0  0.000000e+00         0        
5       0                   0       0  ( < ßëñ \rà  |+  
6       0       ê            0       0   ç|ä.  í+ñè  (¢<      0.0  0.000000e+00         0        
7       0                   0       0  < ßëñ \rà¸u<ñ(  .ñ(äçáï  ø°áä      0.0  0.000000e+00         0        °
8       0                   0       0   ïñë( âíààçñëè    <áë¢ < +.á      0.0  0.000000e+00         0         
9     204       í            0       0    ( < ßëñ   àgB£   çäç||
1    
2   ëñ \ràc !( +
3    ¢<+¢á+¢ ê|(
4    cAcAö"Ö
5   ( < ßëñ \rà
6    ç|ä.
7   < ßëñ \rà¸u<ñ(
8    ïñë( âíààçñëè
9     ( < ßëñ
10  ñ \ràc@ç||&á+
11    äçáê èñ+å& +è
12   áäægJ"£ç &&ßå êàá+
14   lWjalWCgcÃ0aëñáï
15   .í < <í(&íê
16   Õ°ÑQ
         aèçá.í < <í(&íê
17   ( < ßë
18   â èíäçáê ë
19   &çí çâá+åëáá

============================================================
PROGRAM COMPLETED SUCCESSFULLY!
============================================================
