============================================================
EIBDLCRM - BNM LCR Reporting (Conventional Banking)
============================================================

NOTE: KALMLIQ logic integrated directly
      - Reading from BNMK.K1TBL{mon}{week} and BNMK.K3TBL{mon}{week}
      - Using hardcoded FX rates
============================================================

Report Date: 31/07/2026
Week: 4, Month: 07
Expected K1/K3 files: k1tbl074.sas7bdat

============================================================
LOADING INPUTS
============================================================

1. FX Rates (HARDCODED)...
  Loaded 10 currencies: ['MYR', 'USD', 'SGD', 'HKD', 'AUD', 'JPY', 'XAU', 'GBP', 'EUR', 'CNY']

2. Loading WALK.TXT and TEMPL.TXT...
    Warning: Could not read /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/list/walk.txt: invalid literal for int() with base 10: '1F144611FXS'
    Read 70 records from templ.txt
  WALK: 3 records
  TEMPL: 70 records

3. Processing KALMLIQ (K1TBL and K3TBL)...
  Looking for K1TBL file...
    Base path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/
    Month: 07, Week: 4
    Looking for exact matches:
      k1tbl074.sas7bdat: ✓ Found
  Using K1TBL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/k1tbl074.sas7bdat
    Successfully read: k1tbl074.sas7bdat (7199 rows, 61 columns)
  Processing K1TBL with 7199 rows...
    Columns: ['REPTDATE', 'GWAB', 'GWAN', 'GWAS', 'GWAPP', 'GWACS', 'GWBALA', 'GWBALC', 'GWPAIA', 'GWPAIC']...
    Column 'gwmvt' not found! Available columns: ['REPTDATE', 'GWAB', 'GWAN', 'GWAS', 'GWAPP', 'GWACS', 'GWBALA', 'GWBALC', 'GWPAIA', 'GWPAIC', 'GWSHN', 'GWCTP', 'GWACT', 'GWACD', 'GWSAC', 'GWNANC', 'GWCNAL', 'GWCCY', 'GWCNAR', 'GWCNAP', 'GWDIAA', 'GWDIAC', 'GWCIAA', 'GWCIAC', 'GWRATD', 'GWRATC', 'GWDIPA', 'GWDIPC', 'GWCIPA', 'GWCIPC', 'GWPL1D', 'GWPL2D', 'GWPL1C', 'GWPL2C', 'GWPALA', 'GWPALC', 'GWDLP', 'GWDLR', 'GWSDT', 'GWRDT', 'GWRRT', 'GWPDT', 'GWPRT', 'GWPCM', 'GWMOTC', 'GWMRTC', 'GWMRT', 'GWMDT', 'GWMCM', 'GWMWM', 'GWMVT', 'GWMVTS', 'GWSRC', 'GWUC1', 'GWUC2', 'GWC2R', 'GWAMAP', 'GWEXR', 'GWOPT', 'GWOCY', 'GWCBD']
    Found similar column: GWMVT
    Unique values in GWMVT: ['', 'P']
    Found similar column: GWMVTS
    Unique values in GWMVTS: ['S', '', 'M', 'P']
    Sample rows (first 3):
      Row 1:
      Row 2:
      Row 3:
    Filtered row 1: gwmvt='' (expected 'P')
    Filtered row 2: gwmvt='' (expected 'P')
    Filtered row 3: gwmvt='' (expected 'P')
  K1TBL processing stats:
    Total rows: 7199
    Filtered out (GWMVT != 'P'): 7199
    Passed GWMVT = 'P': 0
    Excluded (XAU/XAT currency): 0
    Records with item assigned: 0
  K1TBL records: 0
  Looking for K3TBL file...
    Base path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/
    Month: 07, Week: 4
    Looking for exact matches:
      k3tbl074.sas7bdat: ✓ Found
  Using K3TBL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/k3tbl074.sas7bdat
    Successfully read: k3tbl074.sas7bdat (20171 rows, 39 columns)
  Processing K3TBL with 20171 rows...
    Columns: ['REPTDATE', 'UTSTY', 'UTREF', 'UTDLP', 'UTDLR', 'UTSMN', 'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV']...
    Column 'utref' not found! Available columns: ['REPTDATE', 'UTSTY', 'UTREF', 'UTDLP', 'UTDLR', 'UTSMN', 'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV', 'UTIDT', 'UTLCD', 'UTNCD', 'UTMDT', 'UTCBD', 'UTCPR', 'UTQDS', 'UTPCP', 'UTAMOC', 'UTDPF', 'UTAICT', 'UTAICY', 'UTAIT', 'UTDPET', 'UTDPEY', 'UTDPE', 'UTASN', 'UTOSD', 'UTCA2', 'UTSAC', 'UTCNAP', 'UTCNAR', 'UTCNAL', 'UTCCY', 'UTAMTS', 'MATDT', 'ISSDT', 'DDATE', 'XDATE']
    Found similar column: UTREF
    Unique values in UTREF: ['DRI', 'DLG', 'ISV', 'AFS', 'INV', 'AFSLIQ', 'PSD', '']
    Sample rows (first 3):
      Row 1:
      Row 2:
      Row 3:
  K3TBL processing stats:
    Total rows: 20171
    Rows matching UTREF patterns: 0
    Records with item assigned: 0
  K3TBL records: 0
  Total treasury records: 0

4. Processing DCIWH.DCID...
  Using DCI file: dcid0731.sas7bdat
    Successfully read: dcid0731.sas7bdat (230 rows, 33 columns)
    Sample rows (first 3):
      Row 1:
      Row 2:
      Row 3:
  DCI records: 0

5. Processing CIS.CUSTDLY (parquet)...
  No CIS parquet files found
  CIS records: 0

6. Processing EQUA.UTMS/UTFX/UTRP...
    Successfully read: utms260731.sas7bdat (20096 rows, 99 columns)
    Successfully read: utfx260731.sas7bdat (6061 rows, 39 columns)
    Successfully read: utrp260731.sas7bdat (66 rows, 79 columns)
  UTSAS records: 0

7. Processing LCR.FD/SA/CA/FCYCA...
    Successfully read: fdhold.sas7bdat (57 rows, 8 columns)
    Successfully read: fd30.sas7bdat (2656534 rows, 11 columns)
    Successfully read: sa30.sas7bdat (4238423 rows, 7 columns)
    Successfully read: ca30.sas7bdat (825297 rows, 9 columns)
    Successfully read: fcyca30.sas7bdat (71165 rows, 9 columns)
  Banking records: 7,791,476

8. Processing CISDP/CISCA.DEPOSIT...
  CIS info records: 0

9. Processing LIST.LCR_ECP...
    Successfully read: lcr_ecp_08.sas7bdat (86532 rows, 8 columns)
    Successfully read: lcr_ecp.sas7bdat (92893 rows, 8 columns)
  ECP records: 0

============================================================
PROCESSING DATA
============================================================

Combined treasury + DCI: 0 records
Enhanced treasury: 0 records
Enhanced banking: 7,791,476 records

Applying insurance split...
Banking after insurance split: 7,791,476 records

Total records before consolidation: 7,791,476

Consolidating...
  Consolidated to 1 BNM code x currency combinations

Generating LCR report (text format)...
  ✓ lcr31.txt: 1 items x 1 columns

============================================================
SUMMARY
============================================================

Total: RM 0K

By Source:
  banking_sa: RM 0K
  banking_ca: RM 0K
  banking_fcyca: RM 0K
  banking_fd: RM 0K

============================================================
✓ EIBDLCRM Complete
============================================================
