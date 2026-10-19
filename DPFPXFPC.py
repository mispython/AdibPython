Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDLIBT.py", line 5, in <module>
    df, meta = pyreadstat.read_sas7bdat(
  File "pyreadstat/pyreadstat.pyx", line 129, in pyreadstat.pyreadstat.read_sas7bdat
  File "pyreadstat/_readstat_parser.pyx", line 1110, in pyreadstat._readstat_parser.run_conversion
pyreadstat._readstat_parser.PyreadstatError: output format must be one of {'dict', 'pandas'}, 'polars' was given
