Starting LONPAC data processing...
Processing PA data...
sys:1: UserWarning: CSV malformed: expected 1 rows, actual 3 rows, in chunk starting at byte offset 714384, length 2706
sys:1: UserWarning: CSV malformed: expected 4 rows, actual 8 rows, in chunk starting at byte offset 714384, length 7216
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 669284, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 625086, length 44198
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 579986, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 534886, length 45100
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 489786, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 444686, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 47 rows, in chunk starting at byte offset 402292, length 42394
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 357192, length 45100
sys:1: UserWarning: CSV malformed: expected 23 rows, actual 49 rows, in chunk starting at byte offset 312994, length 44198
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 268796, length 44198
sys:1: UserWarning: CSV malformed: expected 25 rows, actual 50 rows, in chunk starting at byte offset 223696, length 45100
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 49 rows, in chunk starting at byte offset 179498, length 44198
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 134398, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 89298, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 45100, length 44198
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 0, length 45100
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNPC_LONPAC_UAT.py", line 413, in <module>
    process_lonpac_data(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNPC_LONPAC_UAT.py", line 387, in process_lonpac_data
    process_pa_data(input_path, output_path, reptyear4)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNPC_LONPAC_UAT.py", line 72, in process_pa_data
    df = df.with_columns([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "ISSUEDTX"; valid columns: ["W23PA96052652KUL        \"", "      ABDUL RAHMAN BIN MD RASIP                                                                     ", "      671016-04-5033    ", "                    ", "                          ", "         16/10/1967    ", "      M         ", "      W26221KUL     ", "      802           ", "                              ", "      M       ", "      1                 ", "             175,000.00    ", "      23/02/2023    ", "       22/02/2024    ", "              98.00    ", "      011-44566221                      ", "      NO 29 JLN TP 2 TMN TROPIKA  PUTRA, OFF JLN BRP 7 BKT RAHMAN PUTRA                             ", "      SUNGAI BULOH       ", "               47000    ", "      PA96            ", "      PERSONAL ACCIDENT (E-ASSIST SMART DRIVER)                       ", "      022023              ", "       23/02/2023    ", "                                                                     \r"]
