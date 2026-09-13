REPORT ID : EIBDLNSA
Report Date: 12/09/2026
Reading NFEEFILE ...
Traceback (most recent call last):
  File "/stgsrcsys/host/uat/python/EIBDLNS2.py", line 67, in <module>
    df_fee, _ = pyreadstat.read_sas7bdat(feefile_path, usecols=fee_cols)
  File "pyreadstat/pyreadstat.pyx", line 129, in pyreadstat.pyreadstat.read_sas7bdat
  File "pyreadstat/_readstat_parser.pyx", line 1166, in pyreadstat._readstat_parser.run_conversion
  File "pyreadstat/_readstat_parser.pyx", line 908, in pyreadstat._readstat_parser.run_readstat_parser
  File "pyreadstat/_readstat_parser.pyx", line 830, in pyreadstat._readstat_parser.check_exit_status
pyreadstat._readstat_parser.ReadstatError: Invalid file, or file has unsupported features
NOTE: 11 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/EIBDLNS2.py".
      The minimum record length was 20.
      The maximum record length was 102.
