
============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 23/11/2026  Week: 4  Prev Month: 10
============================================================

STEP 1+2: Reading enrh_ln_note (ENTITY_CD='PIBB', BORSTAT='A')...
  Reading FULL enrh_ln_note.sas7bdat (44 cols)...
  Loaded 8,149,154 rows in 870.6s  RSS 5254MB
  Converted to Polars, RSS 4335MB
  Filtered to 297 rows, RSS 4599MB
  NPLA survivors: 297
  NPLA ready: 297 rows

STEP 3: IIS / SP2...
  Reading: iis.sas7bdat - 135,725 rows, 0.7s
  Reading: sp2.sas7bdat - 135,725 rows, 0.9s
  IIS: 135,725  SP2: 135,725
  WARNING: IIS and SP2 have identical row counts (135,725). Verify paths: iis.sas7bdat vs sp2.sas7bdat

STEP 4: Combine NPL...
  NPL combined: 135,924

STEP 5: CCRIS...
  WARNING: missing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac1126.sas7bdat
  WARNING: missing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/ICREDMSUBAC1126.sas7bdat
  WARNING: missing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac11026.sas7bdat
  WARNING: missing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/ICREDMSUBAC11026.sas7bdat
  CCRIS: 0

STEP 6+7: MERGE NPL/CREDSUB/LOAN + derived fields...
  LOAN ready: 135,924

STEP 9: Customer names...
  Reading: loan.sas7bdat - 13,557,834 rows, 534.2s
  CNAME: 12,580,070

STEP 10: Guarantors...
  WARNING: missing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/lnliab07226.sas7bdat
  Guarantor keys: 0

STEP 11: Previous balance...
  WARNING: missing /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/loan104.sas7bdat
  SASLN: 0

STEP 12: Final merge + filter...
  WOFF pre-filter: 135,924
  WOFF after filter: 0

STEP 13: Writing outputs...
No accounts for write-off — creating empty files.

============================================================
COMPLETED IN 1512.0s
============================================================
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftext.txt
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftex1.txt
