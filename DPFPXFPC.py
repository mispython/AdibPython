Report date: 31/08/2026
Input files:
  CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m08.sas7bdat
  LIMIT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m08.sas7bdat
  COLL: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
  DESC: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831

Reading SAS datasets...
Found CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m08.sas7bdat
Found LIMIT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m08.sas7bdat
Found CISDP: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/deposit.sas7bdat
Found NPLA: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/npla.sas7bdat
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
COLL parsed: 98 rows
COLL sample data:
                      CCOLLNO        ACCTNO
0   0508000000000000000000000  940000159406
1   0903000126000000000000000   11400022004
2   0903000126000000000000000   11400022004
3   0903000048600000000000000    8700003901
4   0903000048600000000000000    8000000404
5   0903000048600000000000000    8000000404
6   0903000048600000000000000    8000000404
7   0903000320000000000000000    2993011314
8   0903000320000000000000000    2993014627
9   0903000320000000000000000    2993012734
10  0903000320000000000000000    2993014202
11  0903000063700000000000000    2993011120
12  0903000063700000000000000    2033019131
13  0903000063700000000000000    2993014008
14  0903000063700000000000000    2033018002
15  0903000063700000000000000    2993013826
16  0903000063700000000000000    2993013632
17  0903000063700000000000000    2993011933
18  0903000063700000000000000    2993015234
19  0903000063700000000000000    2993013304
COLL filtered for CA accounts: 0 rows

Reading DESC file (EBCDIC)...
Successfully decoded LCCRISEX_DESC_20260831 with cp037
DESC lines with RI or IC: 56731
Sample RI/IC lines:
  '00000000133008 2976                                                                            DNP                                                                   '
  '
    FN                                                                                                          80100 C       
                                                                                                                              MY      &
                                                                                                                                               0000000027112025G04042023                '
  '
   MY      
           I       0000000031052016C15122015                       17                                      JALAN KESUMA 2/1                        BANDAR TASIK KESUMA                     SEMEN'
  '
   I       0000000012072023C14012022                       23                                                                              NEW WORLD COMMERCIAL CENTRE, D                               '
  '
    FN                                        TAMAN KIM LAM - LOT 16                                            88300         
                                                                                                                              MY      
                                                                                                                                      C       0000000029112024N30112022'
DESC parsed (fixed-width): 20119 rows
DESC sample data:
        CCOLLNO CINSTCL NATGUAR      CENSUS
0   00000000133      29                    
1         
          MY      15                    
2        
          FN       N       M         115
3    
       NTEAM      AR      AC   -05   BDR
4         -
           MY      16                    
5   
    MY             3                    
6         
          MY      16              OHOR
7         
          MY      16             JOHOR
8                 B'      RT            
9                 B'      RT            
10                B'      RT            
11       &
          MY      01                  
12        
          MY      21                 PUR
13          &       Y       N            
14  
      NMEDAN      BU       A           3
15         
                                  1
16        -
           MY      01                    
17        
          MY      23           T HOMES
18                                  1
19        
          MY      02          UNTUNG SEL

DESC CINSTCL values:
CINSTCL
22    3962
00    1698
N     1329
23     984
02     604
SI     331
AR     294
A      275
3      236
0      234
S      230
B      214
P      209
AN     207
24     181
HD     178
BH     177
D      171
R      158
01     157
Name: count, dtype: int64

DESC NATGUAR values:
NATGUAR
      9427
     369
PE     365
NS     318
N      276
I      259
AS     254
OC     224
ER     217
WE     207
IA     182
B      169
AN     169
E      169
RI     162
A      145
S      145
LA     144
SI     140
NG     139
Name: count, dtype: int64

DESC filtered: 251 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:396: SettingWithCopyWarning: 
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  desc_filtered['CCOLLNO_CLEAN'] = desc_filtered['CCOLLNO'].str.lstrip('0')

COLL merged with DESC: 0 rows
WARNING: No matches between COLL and DESC
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:483: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])

DEP after CVAR02 mapping: 67 rows

Writing output to DPNPGS_08.sas7bdat...
Final dataset: 67 rows
SAS Connection established. Subprocess id is 604930

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 604930
Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat
Total records: 67
