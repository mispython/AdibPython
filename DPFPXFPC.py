Using HPD from PBBLNFMT.HPD: [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
Global variables: {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '09', 'REPTMON1': '09', 'REPTYEAR': '2026', 'REPTDAY': '30', 'RDATE': '300926', 'SDATE': '230926'}
Reading LNNOTE file: enrh_ln_note_m09.sas7bdat

======================================================================
DIAGNOSTIC [enrh_ln_note_m09.sas7bdat [conventional]]  — rows in chunk: 500,000
======================================================================
columns: ['ACCTNO', 'NAME', 'LOANTYPE', 'NTBRCH', 'ISSUEDT', 'COLLDESC', 'PAIDIND', 'BALANCE', 'ENTITY_CD']

--- LOANTYPE ---
dtype: Float64
nulls: 0
distinct count: 183
sample distinct (first 30): [4, 5, 6, 7, 15, 20, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 60, 61, 70, 71, 72, 73, 74, 77, 78, 103, 104, 128, 135, 141]
  in HPD_VALUES [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]: 2094
  in HP_VALUES  [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]:  2400

--- PAIDIND ---
dtype: String
value counts (top 10): [{'PAIDIND': 'P', 'len': 458280}, {'PAIDIND': 'M', 'len': 41705}, {'PAIDIND': 'C', 'len': 15}]
  != 'P' (raw): 41720
  != 'P' (trim+upper): 41720

--- BALANCE ---
dtype: Float64
nulls: 0
min: -181716.38, max: 222199495.8941277
  < 0: 222567
  == 0: 13401
  > 0: 264032

--- ENTITY_CD ---
dtype: String
value counts (top 10): [{'ENTITY_CD': '', 'len': 499973}, {'ENTITY_CD': 'PIBB', 'len': 27}]
  == 'PIBB' (exact):    27
  == 'PIBB' (trim):     27
  != 'PIBB' (trim):     499973

--- ISSUEDT ---
dtype: Float64
sample raw (first 10): [14633.0, 14435.0, 14432.0, 15578.0, 16714.0, 20328.0, 15943.0, 16532.0, 15551.0, 16938.0]
parsed ISSDT  sample: [datetime.date(2000, 1, 24), datetime.date(1999, 7, 10), datetime.date(1999, 7, 7), datetime.date(2002, 8, 26), datetime.date(2005, 10, 5)]
parsed ISSMTH sample: [1, 7, 7, 8, 10, 8, 8, 4, 7, 5]
parsed ISSYR  sample: [2000, 1999, 1999, 2002, 2005, 2015, 2003, 2005, 2002, 2006]
  ISSMTH distribution: [{'ISSMTH': 1, 'len': 44106}, {'ISSMTH': 2, 'len': 34201}, {'ISSMTH': 3, 'len': 45916}, {'ISSMTH': 4, 'len': 40477}, {'ISSMTH': 5, 'len': 40152}, {'ISSMTH': 6, 'len': 43125}, {'ISSMTH': 7, 'len': 44009}, {'ISSMTH': 8, 'len': 43932}, {'ISSMTH': 9, 'len': 43873}, {'ISSMTH': 10, 'len': 42674}, {'ISSMTH': 11, 'len': 38797}, {'ISSMTH': 12, 'len': 38738}]
  ISSYR  distribution: [{'ISSYR': 1981, 'len': 13}, {'ISSYR': 1982, 'len': 28}, {'ISSYR': 1983, 'len': 50}, {'ISSYR': 1984, 'len': 113}, {'ISSYR': 1985, 'len': 171}, {'ISSYR': 1986, 'len': 89}, {'ISSYR': 1987, 'len': 114}, {'ISSYR': 1988, 'len': 445}, {'ISSYR': 1989, 'len': 1251}, {'ISSYR': 1990, 'len': 1762}, {'ISSYR': 1991, 'len': 1286}, {'ISSYR': 1992, 'len': 966}, {'ISSYR': 1993, 'len': 1657}, {'ISSYR': 1994, 'len': 2945}, {'ISSYR': 1995, 'len': 5069}, {'ISSYR': 1996, 'len': 6726}, {'ISSYR': 1997, 'len': 9013}, {'ISSYR': 1998, 'len': 8680}, {'ISSYR': 1999, 'len': 16096}, {'ISSYR': 2000, 'len': 19589}, {'ISSYR': 2001, 'len': 21767}, {'ISSYR': 2002, 'len': 37830}, {'ISSYR': 2003, 'len': 55609}, {'ISSYR': 2004, 'len': 74293}, {'ISSYR': 2005, 'len': 75410}, {'ISSYR': 2006, 'len': 51913}, {'ISSYR': 2007, 'len': 21316}, {'ISSYR': 2008, 'len': 16615}, {'ISSYR': 2009, 'len': 14178}, {'ISSYR': 2010, 'len': 10911}, {'ISSYR': 2011, 'len': 8781}, {'ISSYR': 2012, 'len': 7530}, {'ISSYR': 2013, 'len': 7230}, {'ISSYR': 2014, 'len': 6296}, {'ISSYR': 2015, 'len': 5758}, {'ISSYR': 2016, 'len': 3747}, {'ISSYR': 2017, 'len': 1346}, {'ISSYR': 2018, 'len': 917}, {'ISSYR': 2019, 'len': 779}, {'ISSYR': 2020, 'len': 516}, {'ISSYR': 2021, 'len': 299}, {'ISSYR': 2022, 'len': 226}, {'ISSYR': 2023, 'len': 246}, {'ISSYR': 2024, 'len': 194}, {'ISSYR': 2025, 'len': 163}, {'ISSYR': 2026, 'len': 67}]
  ISSMTH == 9: 43873
  ISSYR  == 2026: 67
  BOTH (ISSMTH==9 & ISSYR==2026): 8

--- FILTER FUNNEL (conventional, i.e. ENTITY_CD != 'PIBB' trim) ---
  total rows:                              500,000
  after ENTITY_CD != 'PIBB':               499,973
  after LOANTYPE in HPD:                   2,093
  after PAIDIND != 'P':                    9
  after BALANCE > 0:                       9
  after ISSMTH==9 & ISSYR==2026:           0
======================================================================
