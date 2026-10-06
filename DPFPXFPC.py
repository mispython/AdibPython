KALMLIQ loaded from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py
============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 3938403


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
    [build_kalmliq] ktbl rows: 2484
  Raw k_records: 2484
  UTSAS records: 3003
  Treasury: 2484 records

Banking...
  fd: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fd05.sas7bdat
  sa: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/sa05.sas7bdat
  ca: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/ca05.sas7bdat
  fcyca: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr/fcyca05.sas7bdat
  ecp warning: File /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/list/lcr_ecp.sas7bdat does not exist!
  Banking: 2783004 records

Insurance split...
Total: 2991741 records
Summary: 41 codes
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py:543: DeprecationWarning: the argument `columns` for `DataFrame.pivot` is deprecated. It was renamed to `on` in version 1.0.0.
  pivot = final.pivot(index='item', columns='col', values='amt', aggregate_function='sum').fill_null(0)
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Report (sas7bdat): lcr05.sas7bdat
Report (text): lcr05.txt

Total: RM 142,009,936K
============================================================
EIIDLCRM Complete
SAS Connection terminated. Subprocess id was 3938403


note that i do have lcr_ecp dataset, just ignore that one
