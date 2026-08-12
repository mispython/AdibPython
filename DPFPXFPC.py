============================================================
EIIMTLCR - Top Depositors Report (Islamic Banking)
============================================================

Report Date: 01/08/2026
WARNING: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/list/keep_top_dep_excl_pibb.sas7bdat
WARNING: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/list/keep_top_dep_excl_equ_pibb.sas7bdat
Exclusions: CIS=0, EQU=0

Processing M&I...
WARNING: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/cmm08.sas7bdat
  M&I Summary: 0 groups, Detail: 0 records
Processing Equity...
WARNING: File not found: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/equ08.sas7bdat
  Equity Summary: 0 groups, Detail: 0 records

Consolidating...
  Consolidated: 0 groups

Generating reports...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTLCR.py", line 615, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTLCR.py", line 587, in main
    ind_top = generate_top50_report(allsrc, 'I', 'Individual', rep_vars, ind_file)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTLCR.py", line 343, in generate_top50_report
    top50 = allsrc[allsrc['custype'] == cust_type].nlargest(50, 'tot2').copy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/range.py", line 417, in get_loc
    raise KeyError(key)
KeyError: 'custype'
