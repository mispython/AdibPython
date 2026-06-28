 SAS FILE DIAGNOSTIC TOOL
================================================================================

================================================================================
DIAGNOSING: cisdepxn.sas7bdat
================================================================================
📁 File size: 15,172,108,288 bytes (14469.25 MB)
🔒 Readable: True
🔒 Writable: True
📅 Last modified: 2026-06-28 14:12:16.609770

📊 File header analysis:
   Magic bytes: 000000000000000000000000c2ea8160
   ✓ Valid SAS7BDAT signature detected
   ✓ No compression

📖 Attempting to read file...
   Trying: Standard read... ❌ Failed: Unable to read from file
   Trying: With encoding='latin1'... ❌ Failed: Unable to read from file
   Trying: With encoding='utf-8'... ❌ Failed: Unable to read from file
   Trying: With rows_limit=1000... ❌ Failed: read_sas7bdat() got an unexpected keyword argument 'rows_limit'
   Trying: With low_memory=True... ❌ Failed: read_sas7bdat() got an unexpected keyword argument 'low_memory'
   Trying: With formats as pandas... ❌ Failed: read_sas7bdat() got an unexpected keyword argument 'formats_as_dataframe'

❌ All methods failed to read the file
   Trying: pandas.read_sas()... ❌ Failed: Length of values (6117754) does not match length of index (4621078)

📁 Checking for associated files:

📁 Directory information:
   Directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2
   Directory readable: True
   Directory writable: True

📋 All SAS files in directory:
   ✓ cisdepd.sas7bdat (14215.62 MB)
   ✓ cisdepxn.sas7bdat (15293.75 MB)

================================================================================
COMPARISON WITH WORKING FILE
================================================================================

================================================================================
DIAGNOSING: cisdepd.sas7bdat
================================================================================
📁 File size: 14,906,163,200 bytes (14215.62 MB)
🔒 Readable: True
🔒 Writable: True
📅 Last modified: 2026-06-28 13:53:21.868716

📊 File header analysis:
   Magic bytes: 000000000000000000000000c2ea8160
   ✓ Valid SAS7BDAT signature detected
   ✓ No compression

📖 Attempting to read file...
   Trying: Standard read... ✅ SUCCESS!
      Rows: 7,733,240
      Columns: 104
      First 5 columns: ['KEY', 'AMTIND', 'TOTBAL', 'TOTINTPY', 'TOTCURTT']

📁 Checking for associated files:

📁 Directory information:
   Directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQFAR2
   Directory readable: True
   Directory writable: True

📋 All SAS files in directory:
   ✓ cisdepd.sas7bdat (14215.62 MB)
   ✓ cisdepxn.sas7bdat (18069.00 MB)
