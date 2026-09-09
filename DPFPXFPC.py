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
COLL file has 1927828 lines

First 5 lines of COLL file:
Line 0: 'Diß"2976     008
                         Ø

                          Ø
                           ...' (length: 74)
Line 2: '

                  MYR       Diß"Aiß"...' (length: 18)
Line 4: '@ % ç
              4 0904000000000000000000000WINGTM                      29...' (length: 237)
COLL parsed: 1049309 rows
COLL sample data:
          CCOLLNO       ACCTNO
0  09040000000000  00000000000
1  09030000065000  00000000000
2  09030002730000  00000000000
3  05260000000000  00000000000
4  09030006910000  00000000000
5  04270000000000  00000000000
6  10120000000000  00000000000
7  09030000590000  00000000000
8  09250000000000  00000000000
9  08180001100000  00000000000
COLL after filtering valid account numbers: 1045974 rows

Reading DESC file (EBCDIC)...
Successfully decoded LCCRISEX_DESC_20260831 with cp037
DESC file has 58857 lines
DESC parsed: 5 rows
DESC sample data:
          CCOLLNO CINSTCL NATGUAR
0  12026614102024      12      LG
1  11200520102010      06      DR
2  32026723052024      29      TH
3  32022726062020      15      TH
4  12024103092013      18      FL

DESC CINSTCL values:
CINSTCL
12    1
06    1
29    1
15    1
18    1
Name: count, dtype: int64

DESC NATGUAR values:
NATGUAR
TH    2
LG    1
DR    1
FL    1
Name: count, dtype: int64

DESC filtered (NATGUAR in [RI, IC]): 0 rows
WARNING: No valid COLL or DESC records to merge
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:459: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])

DEP after CVAR02 mapping: 67 rows

Writing output to DPNPGS_08.sas7bdat...
Final dataset: 67 rows
SAS Connection established. Subprocess id is 600651

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 600651
Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat
Total records: 67
