Processing date: 2026-09-30 17:04:19.899797
Week: 4, Previous Week: 3
Month: 09, Previous Month: 09
RDate: 30/09/26, SDate: 23/09/26
LNNOTE file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat
HP loan types from PBBLNFMT (17 codes): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]
============================================================
Reading LNNOTE.sas7bdat (only required columns)...
LNNOTE total records: 8,125,473
LNNOTE records after filtering: 1729861
============================================================
Reading NAME8.sas7bdat (only required columns)...
NAME8 records: 5,438,567
============================================================
Reading LOANTEMP.sas7bdat (only required columns)...
ARREAR records: 631,802
============================================================
Merging datasets...
Merged REPO records: 1,729,861
============================================================
Processing REPO data (using PBBELF format functions)...
Processed 1729861 records
============================================================
Splitting into REPO and REPO1...
REPO records (ARREAR >= 10): 301
REPO1 records (LOANTYPE 983,993): 1
============================================================
Sorting by REGNO...
REPO sorted: 301 records
REPO1 sorted: 1 records
============================================================
Creating REPOTXT.txt...
Created REPOTXT file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/REPOTXT.txt
============================================================
Creating REPOTXT1.txt...
Created REPOTXT1 file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI/REPOTXT1.txt
============================================================

============================================================
PROCESSING COMPLETED SUCCESSFULLY!
============================================================
Summary:
  Processing Date: 2026-09-30
  RDate: 30/09/26
  SDate: 23/09/26
  Week: 4, Previous Week: 3
  Month: 09, Previous Month: 09
  HP loan types used: [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]
============================================================
  Total LNNOTE records after filtering: 1729861
  REPO records (ARREAR >= 10): 301
  REPO1 records (983,993): 1
============================================================

Loan type distribution in REPO:
  128.0: 300 records
  983.0: 1 records
============================================================

Sample REPO output (first 3 records):
  1. BRABBR=H10  CAC='NON CAC'  REGNO=AHQ8598        LOANTYPE=128.0  ARREAR=13.0
  2. BRABBR=H10  CAC='NON CAC'  REGNO=AHV1899        LOANTYPE=128.0  ARREAR=13.0
  3. BRABBR=H57  CAC='NON CAC'  REGNO=AJB8657        LOANTYPE=128.0  ARREAR=12.0

Output files saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOI
  - REPOTXT.txt
  - REPOTXT1.txt
You have mail in /var/spool/mail/sas_edw_dev
