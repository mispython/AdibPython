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
CA account numbers (first 20):
[3071098223.0, 3071719525.0, 3071739802.0, 3071792929.0, 3072834609.0, 3074642220.0, 3074835227.0, 3075359720.0, 3075807710.0, 3076605632.0, 3076697730.0, 3088685316.0, 3088853422.0, 3093013110.0, 3094126332.0, 3094190507.0, 3095382834.0, 3096118303.0, 3096381535.0, 3096670514.0]
Processing LIMIT data...
LIMIT processed: 930567 unique records
CA after LIMIT merge: 67 rows
CA after GP3 merge: 67 rows
CA after CISDP merge: 67 rows

Reading COLL file (EBCDIC)...
Successfully decoded LCCRISEX_20260831 with cp037
COLL parsed: 111 rows
COLL sample data (first 20):
           CCOLLNO         ACCTNO
0   09030000794000     0020002408
1   11250000900000   000000000000
2   09030000258000     0891269506
3   09030000258000     0030000214
4   05080000000000  0940000159406
5   05080000000000     0040000237
6   09030001260000    11400022004
7   09030001260000    11400022004
8   09030000486000    08700003901
9   09030000486000    08000000404
10  09030000486000    08000000404
11  09030000486000    08000000404
12  09030000724000     0890179306
13  09030003200000     0980002500
14  09030003200000     0980002500
15  09030003200000     0980002500
16  09030003200000     0010001613
17  09030003200000     2993011314
18  09030003200000     2993014627
19  09030003200000     2993012734
COLL exact matches: 0
COLL last-10 matches: 0
COLL clean matches: 0
COLL matched with CA: 0 rows

Reading DESC file (EBCDIC)...
Successfully decoded LCCRISEX_DESC_20260831 with cp037
DESC parsed: 20119 rows
DESC with NATGUAR='RI': 162 rows
DESC RI sample:
         CCOLLNO CINSTCL NATGUAR      CENSUS
56   
      MNTALAM      SA      RI  21    PANG
150  
       NMUTIA      AR      RI  -02   PANG
208   
       MNMRCB       N      RI         078
295  
      MNTECHN       N      RI            
331   
        NFOCK       N      RI        MEDA
473  
      MNBENCH       N      RI            
501   
       MNDAKA       N      RI            
604  
       NSANJU       N      RI         TMN
727           Ø       S      RI            
843  
       NDEMI-      NG      RI  -07   AMPA

COLL merged with DESC (RI): 0 rows
WARNING: No matches between COLL and DESC RI records
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:479: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])

DEP after CVAR02 mapping: 67 rows

Writing output to DPNPGS_08.sas7bdat...
Final dataset: 67 rows
SAS Connection established. Subprocess id is 610447

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 610447
Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat
Total records: 67
