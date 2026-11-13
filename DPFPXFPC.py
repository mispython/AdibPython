EIBMHPFS: Starting HP FISS disbursement processing...
  CHUNK_SIZE=250,000  MAX_ROWS_TEST=5000000
  EXCLUDE_ENTITY_CD='PIBB'
  [REPTDATE] 2026-10-31  MM=10 YY=2026 DAY=31 WK=4 WK1=3
  [SDATE]    2026-10-01  (ISSDTE >= this)
  [LNNOTE]   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m10.sas7bdat
  [BNM_LOAN] /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01/loan104.sas7bdat
  [enrh_ln_note_m10.sas7bdat] entity column = 'entity_cd'
  [enrh_ln_note_m10.sas7bdat] read 0 chunk(s), 0 raw rows
  HPSETTLE rows: 0
  [loan104.sas7bdat] entity column = 'entity_cd'
  [loan104.sas7bdat] read 0 chunk(s), 0 raw rows
  BNM_LOAN rows: 0
  HPSNR rows: 0
  HP_NEW rows: 0
  HP (final): 0, EXCEPT: 0, HPSNR: 0
  LALM records: 0
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_rdal.txt
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_except.txt
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_snr.txt
EIBMHPFS: Processing complete.
