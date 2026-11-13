EIBMHPFS: Starting HP FISS disbursement processing...
  CHUNK_SIZE=250,000  MAX_ROWS_TEST=1000000
  EXCLUDE_ENTITY_CD='PIBB' (applied to LNNOTE only)
  [REPTDATE] 2026-10-31  MM=10 YY=2026 DAY=31 WK=4 WK1=3
  [SDATE]    2026-10-01  (ISSDTE >= this)
  [LNNOTE]   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m10.sas7bdat
  [BNM_LOAN] /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01/loan104.sas7bdat
  [enrh_ln_note_m10.sas7bdat] 8,125,473 rows, 495 cols
  [enrh_ln_note_m10.sas7bdat] entity column = 'ENTITY_CD'
    filter entity_cd != 'PIBB': dropped 15 rows
    HPSETTLE cumulative kept = 6,767
    filter entity_cd != 'PIBB': dropped 12 rows
    HPSETTLE cumulative kept = 11,664
    filter entity_cd != 'PIBB': dropped 4 rows
    HPSETTLE cumulative kept = 14,405
    filter entity_cd != 'PIBB': dropped 10 rows
    HPSETTLE cumulative kept = 16,405
  [enrh_ln_note_m10.sas7bdat] read 4 chunk(s), 1,000,000 raw rows
  HPSETTLE rows: 16405
  [loan alias] issdte <- issuedt
  [loan alias] custcd <- custcd
  [loan alias] prodcd <- prodcd
  [loan alias] sectorcd <- sectorcd
  [loan104.sas7bdat] 2,664,794 rows, 473 cols
  [loan104.sas7bdat] missing columns: ['issdte', 'issdt', 'issdate', 'issudt']
  [loan104.sas7bdat] read 4 chunk(s), 1,000,000 raw rows
  BNM_LOAN rows: 1000000
  HPSNR rows: 0
  HP_NEW rows: 3
  HP (final): 3, EXCEPT: 0, HPSNR: 0
  LALM records: 40
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_rdal.txt
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_except.txt
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_snr.txt
EIBMHPFS: Processing complete.
