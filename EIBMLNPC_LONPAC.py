Starting LONPAC data processing...
Processing PA data...
sys:1: UserWarning: CSV malformed: expected 1 rows, actual 3 rows, in chunk starting at byte offset 714384, length 2706
sys:1: UserWarning: CSV malformed: expected 5 rows, actual 10 rows, in chunk starting at byte offset 714384, length 9020
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 669284, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 625086, length 44198
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 579986, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 534886, length 45100
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 489786, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 444686, length 45100
sys:1: UserWarning: CSV malformed: expected 25 rows, actual 49 rows, in chunk starting at byte offset 400488, length 44198
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 355388, length 45100
sys:1: UserWarning: CSV malformed: expected 23 rows, actual 49 rows, in chunk starting at byte offset 311190, length 44198
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 266992, length 44198
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 222794, length 44198
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 48 rows, in chunk starting at byte offset 179498, length 43296
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 134398, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 50 rows, in chunk starting at byte offset 89298, length 45100
sys:1: UserWarning: CSV malformed: expected 24 rows, actual 49 rows, in chunk starting at byte offset 45100, length 44198
sys:1: UserWarning: CSV malformed: expected 26 rows, actual 50 rows, in chunk starting at byte offset 0, length 45100
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNPC_LONPAC_UAT.py", line 459, in <module>
    process_lonpac_data(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNPC_LONPAC_UAT.py", line 433, in process_lonpac_data
    process_pa_data(input_path, output_path, reptyear4)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMLNPC_LONPAC_UAT.py", line 71, in process_pa_data
    df = pl.read_csv(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/io/csv/functions.py", line 582, in read_csv
    return _update_columns(df, new_columns)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/io/csv/_utils.py", line 37, in _update_columns
    df.columns = list(new_columns)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 885, in columns
    self._df.set_column_names(names)
polars.exceptions.ShapeError: 28 column names provided for a DataFrame of width 25
