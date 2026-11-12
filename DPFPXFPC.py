Islamic Banking Statistics - 10/11/2026
Processing data for date: 2026-11-10

Input files:
  CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM/intg_dp_acct_current_d10.sas7bdat
  SAVING:  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM/intg_dp_acct_saving_d10.sas7bdat

================================================================================
INSPECTING INPUT DATASETS
================================================================================

SAVING dataset columns (first 20):
  BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ORGCODE, ORGTYPE, INTYTD, FEEPD, PURPOSE, SECTOR, USER2, USER3, RISKCODE, LEDGBAL, OPENIND

CURRENT dataset columns (first 20):
  BRANCH, ACCTNO, NAME, TAXNO, DEBIT, CREDIT, CLOSEDT, REOPENDT, CUSTCODE, ODPLAN, RATE1, RATE2, RATE3, RATE4, RATE5, TODRATE, FLATRATE, BASERATE, ODSTAT, ORGCODE

================================================================================
SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
================================================================================
Loaded CURRENT: 1108836 rows, 149 columns
Loaded SAVING:  6639092 rows, 87 columns

After ENTITY_CD = 'PIBB' filter:
  CURRENT: 159938 rows
  SAVING:  2285493 rows

Using columns:
  BRANCH:  BRANCH
  PRODUCT: PRODUCT
  CURBAL:  CURBAL
  OPENIND: OPENIND
Combined raw data: 2388145 rows
SAS Connection established. Subprocess id is 1251467


80   
81   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
82   
  ✓ Saved SAS dataset: dyibu11.sas7bdat
SAS Connection terminated. Subprocess id was 1251467
Section 1: DYIBU - 267 branches

================================================================================
SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
================================================================================
After ENTITY_CD = 'PIBB' filter:
  SAVING:  2,285,493 rows
  CURRENT: 159,938 rows
Total accounts to process: 2,388,145
Processing accounts using vectorized operations...
  Processed 500,000 accounts...
  Processed 1,000,000 accounts...
  Processed 1,500,000 accounts...
  Processed 2,000,000 accounts...
✓ Processed 2,388,145 accounts

================================================================================
GENERATING OUTPUT DATASETS (SAS7BDAT)
================================================================================

Generating awsa11 (Products 204,215 (Regular Savings))...
SAS Connection established. Subprocess id is 1635691


86   
87   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
88   
  ✓ Saved SAS dataset: awsa11.sas7bdat
SAS Connection terminated. Subprocess id was 1635691
  awsa11 - 10,892 accounts, 415 groups

Generating awsb11 (Product 207 (Islamic Basic Savings))...
SAS Connection established. Subprocess id is 1638030


86   
87   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
88   
  ✓ Saved SAS dataset: awsb11.sas7bdat
SAS Connection terminated. Subprocess id was 1638030
  awsb11 - 175 accounts, 33 groups

Generating awsc11 (Product 214 (Mudharabah by Age/Race))...
SAS Connection established. Subprocess id is 1640291


77   
78   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
79   
  ✓ Saved SAS dataset: awsc11.sas7bdat
SAS Connection terminated. Subprocess id was 1640291
  awsc11 - 9 accounts, 4 groups

Generating mudh11 (Product 214 (Mudharabah by Purpose))...
SAS Connection established. Subprocess id is 1642559


83   
84   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
85   
  ✓ Saved SAS dataset: mudh11.sas7bdat
SAS Connection terminated. Subprocess id was 1642559
  mudh11 - 9 accounts, 5 groups

Generating awca11 (Products 93,96 (Islamic Current Accounts))...
SAS Connection established. Subprocess id is 1644831


102  
103  libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
104  
  ✓ Saved SAS dataset: awca11.sas7bdat
SAS Connection terminated. Subprocess id was 1644831
  awca11 - 0 accounts, 0 groups

Generating awcb11 (Products 160,162,164,168,182,169 (Purpose 1,2,4 only))...
SAS Connection established. Subprocess id is 1647098


86   
87   libname outlib base  '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'  ;
NOTE: Libref OUTLIB was successfully assigned as follows: 
      Engine:        BASE 
      Physical Name: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
88   
  ✓ Saved SAS dataset: awcb11.sas7bdat
SAS Connection terminated. Subprocess id was 1647098
  awcb11 - 245 accounts, 144 groups

================================================================================
ISLAMIC BANKING STATISTICS - COMPLETED
================================================================================

Processing Date: 10/11/2026 (Yesterday)
Completion Time: 2026-11-11 19:30:16

Input Files:
  CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM/intg_dp_acct_current_d10.sas7bdat
  SAVING:  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM/intg_dp_acct_saving_d10.sas7bdat
  Entity filter: ENTITY_CD = 'PIBB'

Format Modules Used:
  PBMISFMT:        ✓ loaded
  PBBDPTFMT_AII:   ✓ loaded

OUTPUT DATASETS (SAS7BDAT only):
1. DYIBU11  - Daily Islamic Balance Summary
   Records: 267
   
2. AWSA11   - Products 204,215 (Regular Savings)
   Records: 415
   
3. AWSB11   - Product 207 (Islamic Basic Savings)
   Records: 33
   
4. AWSC11   - Product 214 (Mudharabah by Age/Race)
   Records: 4
   
5. MUDH11   - Product 214 (Mudharabah by Purpose)
   Records: 5
   
6. AWCA11   - Products 93,96 (Islamic Current Accounts)
   Records: 0
   
7. AWCB11   - Products 160,162,164,168,182,169 (Purpose 1,2,4 only)
   Records: 144

Total Accounts Processed: 2,388,145

Output Files Generated:
- SAS7BDAT files: 14
- Output Directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM


✓ Processing completed successfully!
✓ Output directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM
✓ Data processed for date: 2026-11-10
