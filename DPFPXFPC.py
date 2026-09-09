Report date: 31/08/2026
Output file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat

Reading SAS datasets...
CURRENT dataset: 1132084 rows before filtering
CURRENT dataset: 967521 rows after filtering
LIMIT dataset: 1093140 rows before filtering
LIMIT dataset: 937196 rows after filtering
NPLA dataset: 28 rows
NPLA columns: ['CVAR06', 'CVAR01', 'NDATE', 'STATUS']
NPLA sample:
         CVAR06        CVAR01       NDATE STATUS
0  2.128860e+09  1.000577e+09  30/09/2020       
1  2.145108e+09  1.000583e+09  31/07/2020       
2  2.150605e+09  1.000590e+09  30/06/2026    NPL
3  2.153478e+09  1.000592e+09  31/05/2026    NPL
4  2.154191e+09  1.000592e+09  30/09/2021       

Reading CISDP dataset in chunks...
Required CISDP columns: ['ACCTNO', 'NEWIC', 'CUSTNAME']
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
CISDP dataset: 8666482 unique rows after filtering
CISDP sample data:
         ACCTNO         NEWIC                      CUSTNAME
0  1.273981e+09  671204015798  PRAMALATHA A/P V RANGANATHAN
1  1.383241e+09  671204015798  PRAMALATHA A/P V RANGANATHAN
2  1.805726e+09  671204015798  PRAMALATHA A/P V RANGANATHAN
3  1.827505e+09  671204015798  PRAMALATHA A/P V RANGANATHAN
4  1.834779e+09  671204015798  PRAMALATHA A/P V RANGANATHAN

CA after SCH mapping: 65 rows
Processing LIMIT data...
LIMIT processed: 926298 unique records
CA after LIMIT merge: 65 rows
CA after GP3 merge: 65 rows
CA after CISDP merge: 65 rows
NEWIC non-null: 0
CUSTNAME non-null: 0
DEP after MICR merge: 65 rows
Accounts with arrears: 0
Accounts with NPL: 0

DEP after CVAR02 mapping: 65 rows
DEP after removing duplicates: 65 rows

NPLA columns: ['CVAR06', 'CVAR01', 'NDATE', 'STATUS']
NPLA account column: CVAR06
NPLA census column: CVAR01
NPLA status column: STATUS
NPLA date column: NDATE

Writing output to DPNPGS_08.sas7bdat...
Final dataset: 65 rows
Output columns: ['CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07', 'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14', 'CVAR15']

First 5 records:
       CVAR01 CVAR02 CVAR03 CVAR04 CVAR05      CVAR06 CVAR07    CVAR08  CVAR09      CVAR10  CVAR11 CVAR12 CVAR13 CVAR14  CVAR15
0  3071098223     53                  NaT  3071098223     OD  686000.0     0.0  1841836.54       0                 0233  7072.0
1  3071719525     51                  NaT  3071719525     OD       0.0     0.0    28655.21       0                 0233  6048.0
2  3071739802     51                  NaT  3071739802     OD  700000.0     0.0   191247.76       0                 0233  6048.0
3  3071792929     53                  NaT  3071792929     OD       0.0     0.0       52.21       0                 0233  7081.0
4  3072834609     53                  NaT  3072834609     OD       0.0     0.0   358242.11       0                 0233  8118.0
SAS Connection established. Subprocess id is 616967

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 616967

Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat
Total records: 65
