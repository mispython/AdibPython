============================================================
EIIMTLCR - Top Depositors Report (Islamic Banking)
============================================================

Report Date: 31/07/2026
Exclusions: CIS=99, EQU=75

Processing M&I...
  M&I Summary: 4341078 groups, Detail: 8968532 records
Processing Equity...
  Equity Summary: 967 groups, Detail: 6283 records

Consolidating...
  Consolidated: 4342019 groups

Generating reports...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTLCR.py", line 615, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTLCR.py", line 587, in main
    ind_top = generate_top50_report(allsrc, 'I', 'Individual', rep_vars, ind_file)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIMTLCR.py", line 346, in generate_top50_report
    with open(output_file, 'w') as f:
OSError: [Errno 28] No space left on device: '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUTI.txt'
