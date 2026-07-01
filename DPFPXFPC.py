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
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDMSFX.py:149: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  nlf_combined = pd.concat(nlf_data_filtered, ignore_index=True)

📊 Total records combined: 4

📊 Summary statistics:
  Records: 1
  INDFXFDBAL Total: 4801495.00
  NONFXFDBAL Total: 22714832.00
  INDFXCABAL Total: 281353.00
  NONFXCABAL Total: 2800517.00

📁 Existing records before update: 1
   Records after removing date 24286: 0
   Total records after append: 1

⚠️  Warning: Could not write SAS file: module 'pyreadstat' has no attribute 'write_sas7bdat'
   Updated Parquet and CSV files only:
   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFX/NLF06.parquet
   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFX/NLF06.csv

============================================================
✅ Processing complete! Output saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDMSFX/
============================================================
