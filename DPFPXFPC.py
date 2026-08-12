============================================================
STEP 1: Reading fixed-width file (ASCII format)...
============================================================
File size: 185,874,284 bytes
Record length: 428 bytes
Number of records: 434,285

First record NAME hex: 54414e2041482043484f4f20202020202020202020202020
First record NAME ASCII: TAN AH CHOO
File is in ASCII format, not EBCDIC!

Parsing fixed-width records...
Saving raw bytes to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMADRE/DPADDR_TEMP.parquet...
Step 1 complete!

============================================================
STEP 2: Converting data types...
============================================================
Converting string columns (ASCII)...
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
REPTDATE (yesterday): 2026-08-11

============================================================
SAMPLE DATA (first 10 rows with non-empty NAME):
============================================================
   BANKNO APPCODE       ACCTNO  BRANCH                        NAME        OLDIC         NEWIC  LEDGBAL        CURBAL  YTDBAL  OPENDATE  PRODUCT NAMETYPE
0       0       D            0       0                 TAN AH CHOO      7654002  470612015550      0.0  0.000000e+00     0.0         0      900        1
1       0         -20202020204       0   SIA\r\nDAZMAN  DAVIDSON  &  ¤
3       0                   0       0         Á*Á*Ìì  îá
                                                        1PER B                    0.0  0.000000e+00     0.0         0        0         
4       0                    0       0            MALAYSIA\r\nD  /ON  0000000000      0.0  0.000000e+00     0.0         0        0         
5       0       R            0       0                        HOCK      UNIT 2-  M         JL      0.0  0.000000e+00     0.0         0        0         
6       0                    0       0  LAYSIA\r\nDLIM     KIM CHEW  pDÂ±       0000000      0.0  0.000000e+00     0.0         0        0        
9       0                    0       0  IA\r\nD-|HOO PEN        G YAU  D
8   \r\nD-
9   \r\nDÂ
10     
       1ANG SOCK BEE
11     
       1CHANG JU HOO
12     
       1HOW PAH KEIA
13    
      1RANJANA A/P M
14          
            1AH LOOI
15    
      1AKRAMIN SAN &
16    
      1AMIRUL SHAFIQ
17     
       1ANBAZHAGAN A
18    
      1ANDREW YAP CH
19     
       1ANG BENG TEE
20    
      1ANG CHENG LOO
21     
       1ANG SIEW FAN
22     
       1ANG SIEW KUA
23     
       1ANITA BISWAS
24    
      1ANN TAY GEK L
25    
      1ANNIE TING AH
26      
        1ANUSUYA A/P
27      
        1AU TIEN HEE
28    
      1AU YONG KIM L
29       
         1AW BAY BEE

============================================================
DATA STATISTICS:
============================================================
   total_records  unique_names  unique_accounts  unique_newic  avg_ledger_balance  accounts_with_balance
0         434285        307109             3497        220317        6.607400e+07                 4280.0

============================================================
PROGRAM COMPLETED SUCCESSFULLY!
============================================================
