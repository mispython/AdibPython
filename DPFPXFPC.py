EIBMHPFS: Starting HP FISS disbursement processing...
  CHUNK_SIZE=250,000  MAX_ROWS_TEST=None
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
    filter entity_cd != 'PIBB': dropped 3 rows
    HPSETTLE cumulative kept = 17,684
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 18,730
    HPSETTLE cumulative kept = 18,925
    HPSETTLE cumulative kept = 18,962
    filter entity_cd != 'PIBB': dropped 241,641 rows
    HPSETTLE cumulative kept = 18,962
    filter entity_cd != 'PIBB': dropped 249,993 rows
    HPSETTLE cumulative kept = 18,967
    filter entity_cd != 'PIBB': dropped 72,227 rows
    HPSETTLE cumulative kept = 100,230
    filter entity_cd != 'PIBB': dropped 7 rows
    HPSETTLE cumulative kept = 212,035
    filter entity_cd != 'PIBB': dropped 14 rows
    HPSETTLE cumulative kept = 315,796
    filter entity_cd != 'PIBB': dropped 68 rows
    HPSETTLE cumulative kept = 381,630
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 411,240
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 522,103
    HPSETTLE cumulative kept = 698,897
    HPSETTLE cumulative kept = 936,144
    HPSETTLE cumulative kept = 1,149,499
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 1,354,291
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 1,548,413
    HPSETTLE cumulative kept = 1,768,741
    HPSETTLE cumulative kept = 2,006,550
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 2,240,245
    HPSETTLE cumulative kept = 2,467,951
    HPSETTLE cumulative kept = 2,672,198
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 2,835,705
    filter entity_cd != 'PIBB': dropped 126,109 rows
    HPSETTLE cumulative kept = 2,901,105
    filter entity_cd != 'PIBB': dropped 249,998 rows
    HPSETTLE cumulative kept = 2,901,106
    filter entity_cd != 'PIBB': dropped 249,997 rows
    HPSETTLE cumulative kept = 2,901,106
    filter entity_cd != 'PIBB': dropped 249,996 rows
    HPSETTLE cumulative kept = 2,901,106
    filter entity_cd != 'PIBB': dropped 249,991 rows
    HPSETTLE cumulative kept = 2,901,107
    filter entity_cd != 'PIBB': dropped 125,469 rows
    HPSETTLE cumulative kept = 2,901,107
  [enrh_ln_note_m10.sas7bdat] read 33 chunk(s), 8,125,473 raw rows
  HPSETTLE rows: 2901107
  [loan alias] issdte <- issuedt
  [loan alias] custcd <- custcd
  [loan alias] prodcd <- prodcd
  [loan alias] sectorcd <- sectorcd
  [loan104.sas7bdat] 2,664,794 rows, 473 cols
  [loan104.sas7bdat] missing columns: ['issdte', 'issdt', 'issdate', 'issudt']
  [loan104.sas7bdat] read 11 chunk(s), 2,664,794 raw rows
  BNM_LOAN rows: 2664794
  HPSNR rows: 0
  HP_NEW rows: 13224
  HP (final): 13220, EXCEPT: 4, HPSNR: 0
  LALM records: 3192
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_rdal.txt
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_except.txt
  Written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMHPFS/hp_snr.txt
EIBMHPFS: Processing complete.
