NOWK: 4, NOWK1: 3, REPTMON: 06, RDATE: 300626
CIS sample size: 694 records
HPACC sample size: 500 records
PRODUCT column type: Float64
PRODUCT unique values: [5.0, 15.0, 61.0, 70.0, 71.0, 200.0, 205.0, 210.0, 212.0, 216.0]
BRANCH unique values: [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0]
After filtering: 83 records
Found LKP_BRANCH at: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/LKP_BRANCH
Found LKP_BRANCH at: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/LKP_BRANCH
LKP_BRANCH file not found or empty - using branch codes without abbreviation
Created default branch mapping with 23 records
After CIS merge: 29 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 399, in <module>
    eimhptop()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 304, in eimhptop
    generate_hp_report(hpacc1_df, rdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 361, in generate_hp_report
    mtharr = f"{row.get('MTHARR', 0):,.0f}".rjust(6)
TypeError: unsupported format string passed to NoneType.__format__
