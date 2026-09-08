Reading SAS datasets...
CURRENT dataset: 1132084 rows before filtering
CURRENT dataset: 967521 rows after filtering (ENTITY_CD != 'PIBB')
LIMIT dataset: 1093140 rows before filtering
LIMIT dataset: 937196 rows after filtering (ENTITY_CD != 'PIBB')
NPLA dataset: 28 rows
Reading CISDP dataset in chunks...
CISDP columns available: 81
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
CISDP dataset: 8666482 rows after filtering (SECCUST == '901')
CA after SCH mapping: 65 rows
Processing LIMIT data...
LMTSTART dtype: float64
LMTSTART non-null count: 47547
LIMIT processed: 930567 unique records
CA after LIMIT merge: 67 rows
CA after GP3 merge: 67 rows
CA after CISDP merge: 67 rows
Reading COLL file (EBCDIC)...
COLL file: 1837458 rows
Reading DESC file (EBCDIC)...
DESC file: 58427 rows
DESC after CR mapping: 0 rows
COLL after merge with DESC: 0 rows
Sample CINSTCL values: Series([], Name: count, dtype: int64)
Sample NATGUAR values: Series([], Name: count, dtype: int64)
COLL after filtering (CINSTCL=18, NATGUAR=06): 0 rows
DEP after COLL merge: 0 rows
WARNING: No records after merging with COLL data.
Sample ACCTNO from CA: [3071098223.0, 3071098223.0, 3071098223.0, 3071719525.0, 3071739802.0, 3071792929.0, 3072834609.0, 3074642220.0, 3074835227.0, 3075359720.0]
Sample ACCTNO from COLL: []
Continuing with CA data without collateral merge...
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:433: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])
DEP after CVAR02 mapping: 0 rows
Writing output to DPNPGS_09.sas7bdat...
Final dataset: 0 rows
SAS Connection established. Subprocess id is 493905

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 493905
Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_09.sas7bdat
Total records: 0
