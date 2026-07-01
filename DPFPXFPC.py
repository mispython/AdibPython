
============================================================
Processing date: 2026-06-29
SAS date number: 24286
REPTYEAR: 2026
REPTMON: 06
REPTDAY: 29
RDATE: 26180 (YYDDD format)
============================================================

✅ Loaded 1 records from BEHAVEINDFXFD
✅ Loaded 1 records from BEHAVENONFXFD
✅ Loaded 1 records from BEHAVEINDFXCA
✅ Loaded 1 records from BEHAVENONFXCA
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDMSFX.py:150: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  nlf_combined = pd.concat(nlf_data_filtered, ignore_index=True)

📊 Total records combined: 4

📊 Summary statistics:
  Records: 1
  INDFXFDBAL Total: 4801495.00
  NONFXFDBAL Total: 22714832.00
  INDFXCABAL Total: 281353.00
  NONFXCABAL Total: 2800517.00
Using SAS Config named: default
SAS Connection established. Subprocess id is 1111233

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
   saspy method failed: 'SASsession' object has no attribute 'quit'
   pyreadstat method failed: pyreadstat.write_sas7bdat not available
⚠️  Warning: Could not write SAS file: pyreadstat.write_sas7bdat not available

✅ Created new files:
   MI/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFXSFX/NLF06.parquet
   MI/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFXSFX/NLF06.sas7bdat
   MI/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFXSFX/NLF06.csv

============================================================
✅ Processing complete! Output saved to: MI/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFXSFX/
============================================================
SAS Connection terminated. Subprocess id was 1111233
