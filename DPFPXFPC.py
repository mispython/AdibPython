============================================================
EIMAR301 SAS to Python Conversion - Multi-Report System
============================================================
   PBBLNFMT : PBBLNFMT [/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBLNFMT.py]
   PBBELF   : PBBELF [/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBELF.py]
   HPD      bound via: PBBLNFMT.HPD = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
   ARRCLASS bound via: PBBLNFMT.put(v,'ARRCLASS')
     ARRCLASS( 2) = '1 - < 2 MTH   '   (expected '1 - < 2 MTH   ')
     ARRCLASS( 3) = '2 - < 3 MTH   '   (expected '2 - < 3 MTH   ')
     ARRCLASS(15) = 'DEFICIT       '  (expected 'DEFICIT       ')
   CACBRCH  bound via: PBBELF.format_cacbrch(v)
     CACBRCH(  2) = '911'   (expected '911' - KL)
     CACBRCH( 13) = '000'  (expected '000' - non-CAC)
     CACBRCH( 31) = '000'  (expected '000' - non-CAC)

1. Processing REPTDATE with previous month (datetime/timedelta)...
   Current Date: 20/10/26
   Previous Month Date: 2026-09-01

2. Loading and filtering HP Direct loans...
   [debug] raw LOANTEMP rows: 631802
   [debug] BALANCE>0 rows: 629109
   [debug] BORSTAT != 'Z' rows: 631802
   [debug] sample PRODUCT values: [102, 103, 104, 105, 106, 107, 110, 111, 112, 113, 114, 115, 116, 120, 127]
   [debug] sample BORSTAT values: ['C', 'B', '', 'W', 'P', 'N', 'K', 'M', 'R', 'D', 'G', 'X', 'A', 'F']
   [debug] HPD filter numbers (from PBBLNFMT.HPD): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
   [debug] PRODUCT in HPD rows: 404864
   [debug] rows after all three filters (pre-join): 404862
   [debug] LKP_BRANCH first 8 raw lines (repr):
   [debug]   'B001 PCS   BANK-ATMC                             C                              '
   [debug]   'B002 JSS   JALAN SULTAN SULAIMAN            W    O                              '
   [debug]   'B003 JRC   JALAN RAJA CHULAN                W    O                              '
   [debug]   'B004 MLK   MELAKA                           M    O                              '
   [debug]   'B005 IMO   IPOH MAIN OFFICE                 A    O                              '
   [debug]   'B006 PPG   PULAU PINANG                     P    O                              '
   [debug]   'B007 JBU   JOHOR BAHRU                      J    O                              '
   [debug]   'B008 KTN   KUANTAN                          C    O                              '
   [debug] first 8 parsed rows: [{'BRANCH': 1, 'BRHCODE': 'PCS'}, {'BRANCH': 2, 'BRHCODE': 'JSS'}, {'BRANCH': 3, 'BRHCODE': 'JRC'}, {'BRANCH': 4, 'BRHCODE': 'MLK'}, {'BRANCH': 5, 'BRHCODE': 'IMO'}, {'BRANCH': 6, 'BRHCODE': 'PPG'},{'BRANCH': 7, 'BRHCODE': 'JBU'}, {'BRANCH': 8, 'BRHCODE': 'KTN'}]
   [debug] LKP_BRANCH rows loaded: 376
   [debug] sample branch_df BRANCH values: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
   [debug] sample loan BRANCH values: [13, 14, 17, 27, 29, 31, 34, 48, 49, 52, 55, 59, 63, 65, 69]
   [debug] rows after branch join: 404862
   Filtered HP Direct loans: 404862

3. Generating Report A (EIMAR301-A)...
   Report A accounts: 5946
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 793, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 704, in main
    generate_report_a_summary(report_a_data, variables)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR301.py", line 476, in generate_report_a_summary
    summary_df = pl.DataFrame(summary_data)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 377, in __init__
    self._df = sequence_to_pydf(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 461, in sequence_to_pydf
    return _sequence_to_pydf_dispatcher(
  File "/usr/lib64/python3.9/functools.py", line 888, in wrapper
    return dispatch(args[0].__class__)(*args, **kw)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 718, in _sequence_of_dict_to_pydf
    pydf = PyDataFrame.from_dicts(
polars.exceptions.ComputeError: could not append value: "ALL" of type: str to the builder; make sure that all rows have the same schema or consider increasing `infer_schema_length`

it might also be that a value overflows the data-type's capacity
