Islamic Banking Statistics - 07/07/2026
Processing data for date: 2026-07-07

================================================================================
INSPECTING INPUT DATASETS
================================================================================

SAVING dataset columns (first 20):
  BANKNO, FMTCODE, BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ORGCODE, ORGTYPE, INTYTD, FEEPD, PURPOSE, SECTOR, USER2, USER3, RISKCODE

CURRENT dataset columns (first 20):
  FMTCODE, BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ODPLAN, RATE1, RATE2, RATE3, RATE4, RATE5, TODRATE, FLATRATE, BASERATE, ODSTAT

================================================================================
SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
================================================================================
Loaded CURRENT: 162640 rows, 147 columns
Loaded SAVING: 2298576 rows, 88 columns

Using columns:
  BRANCH: BRANCH
  PRODUCT: PRODUCT
  CURBAL: CURBAL
  OPENIND: OPENIND
Combined raw data: 2394211 rows

Saving dyibu07...
  ✓ Saved Parquet file: dyibu07.parquet
  Creating SAS dataset: dyibu07
SAS Connection established. Subprocess id is 203717


80   
81   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
82   
  ✓ SAS dataset created: dyibu07.sas7bdat
SAS Connection terminated. Subprocess id was 203717
Section 1: DYIBU - 267 branches

================================================================================
SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
================================================================================
Total accounts to process: 2,394,211
Processing accounts using vectorized operations...
  Processed 500,000 accounts...
  Processed 1,000,000 accounts...
  Processed 1,500,000 accounts...
  Processed 2,000,000 accounts...
✓ Processed 2,394,211 accounts

================================================================================
GENERATING OUTPUT DATASETS (SAS7BDAT + PARQUET)
================================================================================

Generating awsa07 (Products 204,215 (Regular Savings))...

Saving awsa07...
  ✓ Saved Parquet file: awsa07.parquet
  Creating SAS dataset: awsa07
SAS Connection established. Subprocess id is 204528


86   
87   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
88   
  ✓ SAS dataset created: awsa07.sas7bdat
SAS Connection terminated. Subprocess id was 204528
  awsa07 - 7,846 accounts, 389 groups

Generating awsb07 (Product 207 (Islamic Basic Savings))...

Saving awsb07...
  ✓ Saved Parquet file: awsb07.parquet
  Creating SAS dataset: awsb07
SAS Connection established. Subprocess id is 204569


86   
87   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
88   
  ✓ SAS dataset created: awsb07.sas7bdat
SAS Connection terminated. Subprocess id was 204569
  awsb07 - 160 accounts, 27 groups

Generating awsc07 (Product 214 (Mudharabah by Age/Race))...

Saving awsc07...
  ✓ Saved Parquet file: awsc07.parquet
  Creating SAS dataset: awsc07
SAS Connection established. Subprocess id is 204594


78   
79   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
80   
  ✓ SAS dataset created: awsc07.sas7bdat
SAS Connection terminated. Subprocess id was 204594
  awsc07 - 10 accounts, 7 groups

Generating mudh07 (Product 214 (Mudharabah by Purpose))...

Saving mudh07...
  ✓ Saved Parquet file: mudh07.parquet
  Creating SAS dataset: mudh07
SAS Connection established. Subprocess id is 204636


84   
85   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
86   
  ✓ SAS dataset created: mudh07.sas7bdat
SAS Connection terminated. Subprocess id was 204636
  mudh07 - 10 accounts, 4 groups

Generating awca07 (Products 93,96 (Islamic Current Accounts))...

Saving awca07...
  ✓ Saved Parquet file: awca07.parquet
  Creating SAS dataset: awca07
SAS Connection established. Subprocess id is 204673


102  
103  libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
104  
  ✓ SAS dataset created: awca07.sas7bdat
SAS Connection terminated. Subprocess id was 204673
  awca07 - 0 accounts, 0 groups

Generating awcb07 (Products 160,162,164,168,182,169 (Purpose 1,2,4 only))...

Saving awcb07...
  ✓ Saved Parquet file: awcb07.parquet
  Creating SAS dataset: awcb07
SAS Connection established. Subprocess id is 204708


86   
87   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
88   
  ✓ SAS dataset created: awcb07.sas7bdat
SAS Connection terminated. Subprocess id was 204708
  awcb07 - 234 accounts, 134 groups

================================================================================
ISLAMIC BANKING STATISTICS - COMPLETED
================================================================================

Processing Date: 07/07/2026 (Yesterday)
Completion Time: 2026-07-08 18:06:07

OUTPUT DATASETS (SAS7BDAT + PARQUET):
1. DYIBU07  - Daily Islamic Balance Summary
   Records: 267
   Files: dyibu07.sas7bdat, dyibu07.parquet
   
2. AWSA07   - Products 204,215 (Regular Savings)
   Records: 389
   
3. AWSB07   - Product 207 (Islamic Basic Savings)
   Records: 27
   
4. AWSC07   - Product 214 (Mudharabah by Age/Race)
   Records: 7
   
5. MUDH07   - Product 214 (Mudharabah by Purpose)
   Records: 4
   
6. AWCA07   - Products 93,96 (Islamic Current Accounts)
   Records: 0
   
7. AWCB07   - Products 160,162,164,168,182,169 (Purpose 1,2,4 only)
   Records: 134

Total Accounts Processed: 2,394,211

Output Files Generated:
- SAS7BDAT files: 7
- Parquet files: 7
- CSV files (backup): 0
- Output Directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM

Product Categories:
- Savings: 204 (Regular), 207 (Basic), 214 (Mudharabah), 215 (Special)
- Current: 93,96 (Basic Islamic), 160-169,182 (Specific Purpose)

Metrics per Dataset:
- NOACCT: Number of accounts
- CURBAL: Total current balance
- ACCYTD: Accounts opened year-to-date
- AVGACCT: Count of accounts with average balance
- AVGAMT: Total average amount


✓ Processing completed successfully!
✓ Output directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
✓ Data processed for date: 2026-07-07
You have mail in /var/spool/mail/sas_edw_dev
