Reading SAS datasets...
CURRENT dataset: 1132084 rows before filtering
CURRENT dataset: 967521 rows after filtering
LIMIT dataset: 1093140 rows before filtering
LIMIT dataset: 937196 rows after filtering
NPLA dataset: 28 rows
Reading CISDP dataset in chunks...
Processed 10 chunks, 1000000 rows total
Processed 20 chunks, 2000000 rows total
Processed 30 chunks, 3000000 rows total
Processed 40 chunks, 4000000 rows total
Processed 50 chunks, 5000000 rows total
Processed 60 chunks, 6000000 rows total
Processed 70 chunks, 7000000 rows total
Processed 80 chunks, 8000000 rows total
Processed 90 chunks, 9000000 rows total
Processed 100 chunks, 10000000 rows total
Processed 110 chunks, 10935758 rows total
CISDP dataset: 8666482 rows after filtering
CA after SCH mapping: 65 rows
Processing LIMIT data...
LIMIT processed: 930567 unique records
CA after LIMIT merge: 67 rows
CA after GP3 merge: 67 rows
CA after CISDP merge: 67 rows

Reading COLL file (EBCDIC)...
Successfully decoded LCCRISEX_20260831 with cp037
COLL parsed: 4884690 rows
COLL sample data:
   REC_TYPE       CCOLLNO        ACCTNO
0          Diß  "2976     0
1       ß"  
2       æ   
              
3            
             ê
                 
                 ê
4       000   00000LANDMR             K
5       ¬30   34     008  r
6       Ä
           
          \@  iÄ

7            1
8          \n
                  æ
9          
              ç
10         1  

11      æ     
                            MYR
12               2097       0000000
13          ç  lË
                  090
14            
15             0000000000  000211650
16                      

17         
           3 090300  156400000000
18           ã¤DáÂ-±
19      á   Â-±AáÂ-±  r
                       eà

COLL record types: REC_TYPE
    854483
       460677
000    426272
     294282
       80905
       77749

     76385
0     72398
     72371
     72356
Name: count, dtype: int64
COLL CCOLLNO sample: ['\x00\x00\x00\x00\x13\x1aD\x03\x7fiß', '\x8d\x9d\x19r\n@\x00\x00\x00\x00\x00', '\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00', '\x00\x0c\x00\x00\x00\x02ê\x00\x0c\x00\x00', '00000LANDMR', '34     008\x00', '\x00\x00\x00\x00\x0c\x86\x08\x80\n\x08@', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x00\x00\x00\x9c\n\x00\x0c\x10\x08\x80', '\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00']
COLL ACCTNO sample: ['\x10"2976     0', '\x10 %\x00\x00\x00\x00\x00\x0c\x01\x87\x80', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', '\x00\x00\x13\x00\x0c\x00\x00\x00\x02ê\x00', 'K', '\x00\x00\x90\x00\x00\x0c\x10\x86\x19r\x1bæ', '\x00\x00\x00\x00i\x00Ä\x0c\x00\x00\x00\x00', '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001', '\x81\x88æ\x00\x00\x00\x00\x00', '\x00\x00\x00\x00\x00\x00\x00 \x00\x00ç\x03']

Reading DESC file (EBCDIC)...
Successfully decoded LCCRISEX_DESC_20260831 with cp037
DESC parsed: 5 rows
DESC sample data:
          CCOLLNO CINSTCL NATGUAR STATE COUNTRY
0  12026614102024      12      LG              
1  11200520102010      06      DR              
2  32026723052024      29      TH              
3  32022726062020      15      TH              
4  12024103092013      18      FL              

DESC CINSTCL values: CINSTCL
12    1
06    1
29    1
15    1
18    1
Name: count, dtype: int64
DESC NATGUAR values: NATGUAR
TH    2
LG    1
DR    1
FL    1
Name: count, dtype: int64

DESC filtered (NATGUAR=RI): 0 rows
DESC filtered (NATGUAR=IC): 0 rows
WARNING: No valid COLL or DESC records to merge
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:426: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])
DEP after CVAR02 mapping: 67 rows

Writing output to DPNPGS_09.sas7bdat...
Final dataset: 67 rows
SAS Connection established. Subprocess id is 596457

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 596457
Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_09.sas7bdat
Total records: 67


make adjustments on the new updated code
