============================================================
EIVD GL PROCESSING STARTED (EIVDNLGL)
============================================================
Processing date: 2026-07-08
Store directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIVDNLGL
============================================================
Total lines: 9

First 5 lines of data:
Line 1: '20260708                                                                        '
  Positions 0-8 (GLITEM): '20260708'
  Positions 20-28 (DATEX): ''
  Positions 45-60 (BALANCE): ''

Line 2: '1S-RCF              08/07/26                    36,353,900.00                                                                        '
  Positions 0-8 (GLITEM): '1S-RCF'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '36,353,900.0'

Line 3: '1S-TLF              08/07/26                   250,737,245.49                                                                        '
  Positions 0-8 (GLITEM): '1S-TLF'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '250,737,245.4'

Line 4: '1S-BA F             08/07/26                     4,353,267.90                                                                        '
  Positions 0-8 (GLITEM): '1S-BA F'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '4,353,267.9'

Line 5: '1S-SM F             08/07/26                             0.00                                                                        '
  Positions 0-8 (GLITEM): '1S-SM F'
  Positions 20-28 (DATEX): '08/07/26'
  Positions 45-60 (BALANCE): '0.0'


Parsed 9 rows from GL file
Columns: ['GLITEM', 'DATEX', 'BALANCE', 'SIGN']

Data sample:
shape: (9, 4)
┌──────────┬──────────┬───────────┬──────┐
│ GLITEM   ┆ DATEX    ┆ BALANCE   ┆ SIGN │
│ ---      ┆ ---      ┆ ---       ┆ ---  │
│ str      ┆ str      ┆ f64       ┆ str  │
╞══════════╪══════════╪═══════════╪══════╡
│ 20260708 ┆ 080726   ┆ 0.0       ┆     │
│ 1S-RCF   ┆ 080726   ┆ 3.63539e7 ┆      │
│ 1S-TLF   ┆ 080726   ┆ 2.5074e8  ┆      │
│ 1S-BA F  ┆ 080726   ┆ 4353267.9 ┆      │
│ 1S-SM F  ┆ 080726   ┆ 0.0       ┆      │
│ 1S-GUARA ┆ 080726   ┆ 5.7e6     ┆      │
│ - ┆  ┆ 0.0       ┆     │
│ 1S-REMIS ┆ 080726   ┆ 0.0       ┆      │
│ 1S-FIXED ┆ 080726   ┆ 0.0       ┆      │
└──────────┴──────────┴───────────┴──────┘

Unique GLITEMs in file (9):
  '-'
  '1S-BA F'
  '1S-FIXED'
  '1S-GUARA'
  '1S-RCF'
  '1S-REMIS'
  '1S-SM F'
  '1S-TLF'
  '20260708'

GL Date from file: 080726
REPT Date: 080726
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVDNLGL.py", line 180, in <module>
    DF_G
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10314, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/lazy.py", line 1088, in __call__
    rv = self.function(slp, *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4655, in _wrap
    return function(sl[0], *args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/expr/expr.py", line 4879, in wrap_f
    return x.map_elements(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/series/series.py", line 5838, in map_elements
    self._s.map_elements(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVDNLGL.py", line 183, in <lambda>
    pl.col("DATEX").map_elements(lambda s: ddmmyy_to_date(s), return_dtype=pl.Date).alias("DATE"),
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIVDNLGL.py", line 130, in ddmmyy_to_date
    dd = int(s[0:2])
ValueError: invalid literal for int() with base 10: '\x00\x00'



BELOW IS THE SAMPLE FROM PIVB NLGL

㈰㈶〷〸††††††††††††††††††††††††††††††††††††                          
਱匭剃䘠††††††‰㠯〷⼲㘠†††††††††″㘬㌵㌬㤰〮〰††††††††††††††††††††††††††††††††††††ഊㅓⵔ䱆†††††††〸⼰㜯㈶†††††††††′㔰ⰷ㌷ⰲ㐵⸴㤠†††††††††††††††††††††††††††††††††††‍਱匭䉁⁆††††††‰㠯〷⼲㘠††††††††††㐬㌵㌬㈶㜮㤰††††††††††††††††††††††††††††††††††††ഊㅓⵓ䴠䘠††††††〸⼰㜯㈶††††††††††††††‰⸰〠†††††††††††††††††††††††††††††††††††‍਱匭䝕䅒䅎呅䔠†††‰㠯〷⼲㘠††††††††††㔬㜰〬〰〮〰††††††††††††††††††††††††††††††††††††ഊⴀ                                                                 
਱匭剅䵉卉䕒䙄†††‰㠯〷⼲㘠††††††††††††††〮〰††††††††††††††††††††††††††††††††††††ഊㅓⵆ䥘䕄⁄䕐††††〸⼰㜯㈶††††††††††††††‰⸰〠†††††††††††††††††††††††††††††††††††‍ਠ††††††††††††††††††††††††††††††††††††††††††††††††††††††††††††††††††ഊ
