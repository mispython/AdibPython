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
PAIDIND counts: [{'PAIDIND': 'P', 'len': 93141}, {'PAIDIND': 'M', 'len': 6853}, {'PAIDIND': 'C', 'len': 6}]
ENTITY_CD counts: [{'ENTITY_CD': '', 'len': 99994}, {'ENTITY_CD': 'PIBB', 'len': 6}]
ISSMTH dist: [{'ISSMTH': 1, 'len': 9008}, {'ISSMTH': 2, 'len': 6775}, {'ISSMTH': 3, 'len': 8951}, {'ISSMTH': 4, 'len': 7960}, {'ISSMTH': 5, 'len': 7958}, {'ISSMTH': 6, 'len': 8555}, {'ISSMTH': 7, 'len': 8793}, {'ISSMTH': 8, 'len': 8573}, {'ISSMTH': 9, 'len': 8820}, {'ISSMTH': 10, 'len': 8952}, {'ISSMTH': 11, 'len': 7838}, {'ISSMTH': 12, 'len': 7817}]
ISSYR dist (top years): [{'ISSYR': 2003, 'len': 13068}, {'ISSYR': 2002, 'len': 12599}, {'ISSYR': 2004, 'len': 11159}, {'ISSYR': 2001, 'len': 9317}, {'ISSYR': 2000, 'len': 9097}, {'ISSYR': 2005, 'len': 7871}, {'ISSYR': 1999, 'len': 7540}, {'ISSYR': 2006, 'len': 4938}, {'ISSYR': 2007, 'len': 3463}, {'ISSYR': 1998, 'len': 3053}]

--- HPD loans that pass PAID+BAL filters, by year-month (top 20) ---
[{'ISSYR': 2019, 'ISSMTH': 3, 'len': 1}]

--- HPD loans by year-month (ignoring PAIDIND/BALANCE), top 20 ---
[{'ISSYR': 2021, 'ISSMTH': 1, 'len': 1}, {'ISSYR': 2020, 'ISSMTH': 8, 'len': 1}, {'ISSYR': 2020, 'ISSMTH': 2, 'len': 1}, {'ISSYR': 2019, 'ISSMTH': 5, 'len': 1}, {'ISSYR': 2019, 'ISSMTH': 3, 'len': 1}, {'ISSYR': 2016, 'ISSMTH': 6, 'len': 1}, {'ISSYR': 2015, 'ISSMTH': 6, 'len': 1}, {'ISSYR': 2015, 'ISSMTH': 4, 'len': 1}, {'ISSYR': 2015, 'ISSMTH': 1, 'len': 1}, {'ISSYR': 2013, 'ISSMTH': 10, 'len': 1}, {'ISSYR': 2013, 'ISSMTH': 8, 'len': 1}, {'ISSYR': 2013, 'ISSMTH': 7, 'len': 1}, {'ISSYR': 2013, 'ISSMTH': 2, 'len': 1}, {'ISSYR': 2013, 'ISSMTH': 1, 'len': 1}, {'ISSYR': 2011, 'ISSMTH': 1, 'len': 1}, {'ISSYR': 2010, 'ISSMTH': 3, 'len': 1}, {'ISSYR': 2007, 'ISSMTH': 1, 'len': 1}, {'ISSYR': 2006, 'ISSMTH': 11, 'len': 1}, {'ISSYR': 2005, 'ISSMTH': 11, 'len': 1}, {'ISSYR': 2004, 'ISSMTH': 8, 'len': 1}]

--- FILTER FUNNEL (islamic) ---
  total rows:                              100,000
  after ENTITY_CD == 'PIBB':               6
  after LOANTYPE in HPD:                   0
  after PAIDIND != 'P':                    0
  after BALANCE > 0:                       0
  after ISSMTH==9 & ISSYR==2026:           0

Combined filtered rows: 0

*** No data after filtering — see the funnel above for which step killed it. ***
Try different months:
  REPTMON=08 REPTYEAR=2026 python EIBMBRAS.py
  REPTMON=07 REPTYEAR=2026 python EIBMBRAS.py

Or check which month has HPD loans by looking at the
'HPD loans by year-month' sections above.
