KALMLIQ loaded from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py
============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 4002251


Date: 05/10/2026 Week:1 Mon:10
Template: 70 items
  CIS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/CIS_CUST_DAILY.parquet
CIS: 17611 records

Treasury...
  k1tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat
  k3tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k3tbl101.sas7bdat
  k1tbl exists: True
  k3tbl exists: True
    [_build_k1tbl] reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat
    [_build_k1tbl] columns: ['REPTDATE', 'GWAB', 'GWAN', 'GWAS', 'GWAPP', 'GWACS', 'GWBALA', 'GWBALC', 'GWPAIA', 'GWPAIC', 'GWSHN', 'GWCTP', 'GWACT', 'GWACD', 'GWSAC', 'GWNANC', 'GWCNAL', 'GWCCY', 'GWCNAR', 'GWCNAP', 'GWDIAA', 'GWDIAC', 'GWCIAA', 'GWCIAC', 'GWRATD', 'GWRATC', 'GWDIPA', 'GWDIPC', 'GWCIPA', 'GWCIPC', 'GWPL1D', 'GWPL2D', 'GWPL1C', 'GWPL2C', 'GWPALA', 'GWPALC', 'GWDLP', 'GWDLR', 'GWSDT', 'GWRDT', 'GWRRT', 'GWPDT', 'GWPRT', 'GWPCM', 'GWMOTC', 'GWMRTC', 'GWMRT', 'GWMDT', 'GWMCM', 'GWMWM', 'GWMVT', 'GWMVTS', 'GWSRC', 'GWUC1', 'GWUC2', 'GWC2R', 'GWAMAP', 'GWEXR', 'GWOPT', 'GWOCY', 'GWCBD']
    [_build_k1tbl] rows: 753
    [_build_k1tbl] GWMDT sample: [None, 24471.0, 24441.0]
    [_build_k1tbl] GWMDT dtype: Float64
    [_build_k1tbl] after filter GWMVT='P': 752
    [_build_k1tbl] emitted rows: 741
    [_build_k3tbl] reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k3tbl101.sas7bdat
    [_build_k3tbl] columns: ['REPTDATE', 'UTSTY', 'UTREF', 'UTBRNM', 'UTDLP', 'UTDLR', 'UTSMN', 'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV', 'UTIDT', 'UTLCD', 'UTNCD', 'UTMDT', 'UTCBD', 'UTCPR', 'UTQDS', 'UTPCP', 'UTAMOC', 'UTDPF', 'UTAICT', 'UTAICY', 'UTAIT', 'UTDPET', 'UTDPEY', 'UTDPE', 'UTASN', 'UTOSD', 'UTCA2', 'UTSAC', 'UTCNAP', 'UTCNAR', 'UTCNAL', 'UTCCY', 'UTAMTS', 'UTMM1', 'MATDT', 'ISSDT', 'DDATE', 'XDATE']
    [_build_k3tbl] rows: 2297
    [_build_k3tbl] MATDT sample: [None, 24997.0, 25056.0]
    [_build_k3tbl] MATDT dtype: Float64
    [_build_k3tbl] emitted rows: 501
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 702, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 462, in main
    k_records = process_treasury(rep_date)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 140, in process_treasury
    ktbl, _dist = build_kalmliq(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py", line 365, in build_kalmliq
    df = pl.DataFrame(ktbl_rows)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 391, in __init__
    self._df = sequence_to_pydf(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 466, in sequence_to_pydf
    return _sequence_to_pydf_dispatcher(
  File "/usr/lib64/python3.9/functools.py", line 888, in wrapper
    return dispatch(args[0].__class__)(*args, **kw)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/construction/dataframe.py", line 722, in _sequence_of_dict_to_pydf
    pydf = PyDataFrame.from_dicts(
polars.exceptions.ComputeError: could not append value: "MYR" of type: str to the builder; make sure that all rows have the same schema or consider increasing `infer_schema_length`

it might also be that a value overflows the data-type's capacity
SAS Connection terminated. Subprocess id was 4002251
