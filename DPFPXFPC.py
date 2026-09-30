REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=29/09/26, SDATE=23/09/26
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 310, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 261, in main
    loan_df = read_sas7bdat(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 158, in read_sas7bdat
    df, _meta = pyreadstat.read_sas7bdat(str(path))
  File "pyreadstat/pyreadstat.pyx", line 129, in pyreadstat.pyreadstat.read_sas7bdat
  File "pyreadstat/_readstat_parser.pyx", line 1104, in pyreadstat._readstat_parser.run_conversion
pyreadstat._readstat_parser.PyreadstatError: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m{REPTMON}.sas7bdat does not exist!
