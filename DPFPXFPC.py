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
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py:292: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.int_range(1, pl.count() + 1).over("BRANCH").alias("N")
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 395, in <module>
    eimhptop()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 306, in eimhptop
    generate_hp_report(hpacc1_df, rdate)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMHPTOP.py", line 334, in generate_hp_report
    f.write(f"BRANCH CODE= {branch:03d}\n")
ValueError: Unknown format code 'd' for object of type 'float'
