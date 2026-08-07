Report Date: 31/07/26
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 311, in <module>
    process_camv()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 187, in process_camv
    df = standardize_columns(df, [
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDOMYR.py", line 133, in standardize_columns
    raise KeyError(
KeyError: "Could not find a match for required column(s) ['CURCODE'].\nColumns actually present in this dataset: ['ACCTNO', 'BRANCH', 'COSTCTR', 'CURBAL', 'CUSTCODE', 'NAME', 'NETBALC', 'NETBALS', 'OPENIND', 'PCURBAL', 'PRODUCT', 'PS']\nAdd the real column name to COLUMN_ALIASES for the missing field(s) above and re-run."


camv and famv inputs are in lowercase
