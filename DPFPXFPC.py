Step 1: Setting report date...
Report Date: 2026-09-08 09:42:14.713059, RDATE: 24357
Step 2: Processing credit facility table...
First few lines of crftabl.txt:
Line 0: '1BKT20260831'
Line 1: 'PBF        1SGXX               JSS/000587/06                                                                                                                   +0000000009000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N1           2500001815'
Line 2: 'PBF        1SGLX               JSS/000325/95                                                                                                                   +0000000009000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N0           2500001912'
Line 3: 'PBF        1LCB2               TDA/000011/06                                                                                                                   +0000000009000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N0           2500003708'
Line 4: 'PBF        1BAX2               JSS/000071/98                                                                                                                   +0000000005300.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00+0000000000000.00N0           2500003902'
Step 3: Merging with master account data...
Step 4: Processing credit data...
Step 5: Summarizing credit outstanding...
Step 6: Processing provision data...
Step 7: Processing subaccount data...
Step 8: Processing collateral data...

thread '<unnamed>' (869266) panicked at /home/runner/work/polars/polars/crates/polars-arrow/src/array/binview/mutable.rs:303:9:
assertion failed: bytes.len() <= u32::MAX as usize
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBTNPGS.py", line 621, in <module>
    desc_data = pl.DataFrame({'data': [line.rstrip('\n') for line in desc_lines]})
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 382, in __init__
    self._df = dict_to_pydf(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 161, in dict_to_pydf
    for s in _expand_dict_values(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 394, in _expand_dict_values
    updated_data[name] = pl.Series(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/series/series.py", line 306, in __init__
    self._s = sequence_to_pyseries(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/series.py", line 341, in sequence_to_pyseries
    return _construct_series_with_fallbacks(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/series.py", line 356, in _construct_series_with_fallbacks
    return constructor(name, values, strict)
pyo3_runtime.PanicException: assertion failed: bytes.len() <= u32::MAX as usize
