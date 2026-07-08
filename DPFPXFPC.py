EIBDAWSA - Average Savings Account Analysis
================================================================================
Report Date: 08/07/2026
Week: 1
Year: 2026, Month: 07, Day: 08
Input file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDAWSA/DPTRBLGS.parquet
File size: 1.74 GB
Initializing DuckDB connection...
Registering parquet file...
Filtering records...
100% ▕██████████████████████████████████████▏ (00:00:02.31 elapsed)     
Filtered records: 8,846,591
ERROR: Invalid Input Error: Could not infer the return type, please set it explicitly
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 389, in <module>
    aggregated_df = process_with_duckdb(input_file, reptdate, reptmon, reptday, reptyear)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDAWSA.py", line 192, in process_with_duckdb
    con.create_function('get_state', get_state_code)
_duckdb.InvalidInputException: Invalid Input Error: Could not infer the return type, please set it explicitly

Completed. Output files in: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA
