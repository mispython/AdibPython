HPD_VALUES: [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
Global variables: {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '09', 'REPTMON1': '09', 'REPTYEAR': '2026', 'REPTDAY': '30', 'RDATE': '300926', 'SDATE': '230926'}

Reading up to 100,000 rows from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m09.sas7bdat
Loaded 100,000 rows

--- FILTER FUNNEL (conventional) ---
  total rows:                              100,000
  after ENTITY_CD != 'PIBB':               99,994
  after LOANTYPE in HPD:                   21
  after PAIDIND != 'P':                    1
  after BALANCE > 0:                       1
  after ISSMTH==9 & ISSYR==2026:           0

--- Column stats ---
LOANTYPE dtype: Float64
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMBRAS.py", line 288, in <module>
    conv_df, islm_df = filter_and_report(raw, reptmon_int, reptyear_int)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMBRAS.py", line 180, in filter_and_report
    print(f"PAIDIND counts: {df.group_by('PAIDIND').len().sort('len', desc=True).to_dicts()}")
TypeError: sort() got an unexpected keyword argument 'desc'
