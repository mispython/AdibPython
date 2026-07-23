============================================================
EIMAR301 SAS to Python Conversion - Multi-Report System
============================================================

1. Processing REPTDATE with previous month (datetime/timedelta)...
   Current Date: 230726
   Previous Month Date: 2026-06-01

2. Loading and filtering HP Direct loans...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 708, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 602, in main
    filtered_loans = load_and_filter_loans(HPD_LIST, variables)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 161, in load_and_filter_loans
    branch_df = load_branch_data()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 121, in load_branch_data
    pdf = pd.read_csv(LKP_BRANCH_PATH, sep=r"\s+", header=None,
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 1026, in read_csv
    return _read(filepath_or_buffer, kwds)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 626, in _read
    return parser.read(nrows)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 1923, in read
    ) = self._engine.read(  # type: ignore[attr-defined]
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/c_parser_wrapper.py", line 234, in read
    chunks = self._reader.read_low_memory(nrows)
  File "parsers.pyx", line 838, in pandas._libs.parsers.TextReader.read_low_memory
  File "parsers.pyx", line 905, in pandas._libs.parsers.TextReader._read_rows
  File "parsers.pyx", line 874, in pandas._libs.parsers.TextReader._tokenize_rows
  File "parsers.pyx", line 891, in pandas._libs.parsers.TextReader._check_tokenize_status
  File "parsers.pyx", line 2061, in pandas._libs.parsers.raise_parser_error
pandas.errors.ParserError: Error tokenizing data. C error: Expected 4 fields in line 2, saw 7


no extension on the LKP_BRANCH. not even a .txt. just pure flat file. 
