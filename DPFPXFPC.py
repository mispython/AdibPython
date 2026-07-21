================================================================================
EIIAQTXT - AQ NPL EXTRACT
================================================================================

LOAN REPORT DATE: 2026-07-20
RDATE: 20072026
NPL6 REPORT DATE: 2026-07-20
NPLDATE: 20072026

================================================================================
VALIDATING DATES
================================================================================
✓ VALIDATION PASSED: NPL DATE (20072026) MATCHES LOAN DATE (20072026)

PROCESSING NPL DATA...
Reading SAS files...
AQ records: 135,478
SP2 records: 135,725
IIS records: 135,725
MERGING AQ, SP2, AND IIS DATA...
AQ MERGED RECORDS: 135,478
AGGREGATING DATA...
AQ SUMMARY RECORDS: 377

GENERATING TEXT FILE...
SAVED: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIAQTXT/AQTXT_20260720.txt

================================================================================
PROCESSING SUMMARY
================================================================================
REPORT DATE: 2026-07-20
TOTAL SUMMARY RECORDS: 377
================================================================================

BREAKDOWN BY LOAN TYPE:
LOANDESC  COUNT  TOTAL_ACCOUNTS    TOTAL_NPL    TOTAL_IIS
   AITAB    377        135478.0 7.886267e+07 1.365346e+06

BREAKDOWN BY RISK CODE:
RISKCD  COUNT  TOTAL_ACCOUNTS    TOTAL_NPL
     B     56          1025.0 8.111224e+06
     D     35           285.0 8.915516e+06
    S1    245        133674.0 4.437951e+07
    S2     41           494.0 1.745643e+07

================================================================================
TEXT FILE FORMAT
================================================================================

FIXED-WIDTH POSITIONS:
  @001-012: RPTDATE (DD/MM/YYYY)
  @014-020: BRANCH (7 chars)
  @023-027: LOANDESC (5 chars - AITAB or HPD)
  @029-030: RISKCD (2 chars - D, L, S1, S2)
  @031-043: NETBALP (13.2 format)
  @044-056: NEWNPL (13.2 format)
  @057-069: RECOVER (13.2 format)
  @070-082: PL (13.2 format)
  @083-095: NPLW (13.2 format)
  @096-108: NPL (13.2 format)
  @109-121: TOTIIS (13.2 format)
  @122-134: SPAMT (13.2 format)
  @135-147: SPPLAMT (13.2 format)
  @148-160: MARKETVL (13.2 format)
  @161-173: ADJUST (13.2 format)

RISK CODES:
  D:  Doubtful
  L:  Loss
  S1: Substandard-1
  S2: Substandard-2 (or other substandard)

LOAN TYPES:
  HPD:   HP Direct Conventional
  AITAB: HP Direct AITAB
================================================================================

PROCESSING COMPLETE!
