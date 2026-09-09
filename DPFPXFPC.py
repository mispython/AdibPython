Report date: 31/08/2026
Output file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat

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
DEP after MICR merge: 67 rows

DEP after CVAR02 mapping: 67 rows

Writing output to DPNPGS_08.sas7bdat...
Final dataset: 67 rows
Output columns: ['CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07', 'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14', 'CVAR15', 'STATUS', 'NDATE']
SAS Connection established. Subprocess id is 614369

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 614369

Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat
Total records: 67
Output columns: ['CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07', 'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14', 'CVAR15']

First 5 records:
       CVAR01 CVAR02 CVAR03 CVAR04               CVAR05      CVAR06 CVAR07    CVAR08  CVAR09      CVAR10  CVAR11 CVAR12 CVAR13 CVAR14  CVAR15
0  3071098223     53    NaN    NaN                  NaT  3071098223     OD  686000.0     0.0  1841836.54       0           NaN   0233  7072.0
1  3071098223     53    NaN    NaN  2000-02-12 00:00:00  3071098223     OD  686000.0     0.0  1841836.54       0           NaN   0233  7072.0
2  3071098223     53    NaN    NaN  2000-02-03 00:00:00  3071098223     OD  686000.0     0.0  1841836.54       0           NaN   0233  7072.0
3  3071719525     51    NaN    NaN                  NaT  3071719525     OD       0.0     0.0    28655.21       0           NaN   0233  6048.0
4  3071739802     51    NaN    NaN                  NaT  3071739802     OD  700000.0     0.0   191247.76       0           NaN   0233  6048.0
