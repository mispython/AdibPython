======================================================================
EIIMLCRM - BNM LCR Reporting (Islamic Banking)
Python conversion of SAS EIIMLCRM program
======================================================================

Report Date: 31/07/2026
Week of Month: 4
Month: 07
Year: 26
Processing Treasury (ALLEQU)...
  UTSAS: 2917 records
  Combined K1TBL+K3TBL: 1162 records
  Treasury processed: 1162 records
Processing Core Banking (ALLMNI)...
  Combined banking: 2792341 records
  CIS info: 9771249 accounts
  Merging CIS/ECP/SME data...
  Processing records...
    Processed 500,000 records...
    Processed 1,000,000 records...
    Processed 1,500,000 records...
    Processed 2,000,000 records...
    Processed 2,500,000 records...
  Banking processed: 2792341 records

Total combined records: 2,793,503
Applying SME reclassification and insurance split...
  After reclassification/split: 3028860 records
Processing NSFR and FD hold...
Applying SHAREX format...
Processing GL data (WALK.TXT)...
  GL records: 2
Generating LCR reports...
  Generated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMLCRM/LCRMTH07.txt
  Generated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMLCRM/LCRUSD07.txt
  Generated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMLCRM/LCRSGD07.txt
  Generated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMLCRM/LCRMYR07.txt
  Reports generated successfully

======================================================================
PROCESSING SUMMARY
======================================================================

Total Amount (RM'000): 110,673,965.30

By Source:
  BANKING: RM 83,517,956.12K
  TREASURY: RM 27,156,009.18K

By Currency:
  AUD: RM 55.21K
  EUR: RM 4,256.59K
  GBP: RM 278.23K
  MYR: RM 110,663,982.61K
  NZD: RM 0.00K
  USD: RM 5,392.66K

======================================================================
EIIMLCRM Complete
======================================================================
