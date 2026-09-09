Step 1: Setting report date...
Report Date: 2026-09-08 19:24:26.039266, RDATE: 24357
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
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBTNPGS.py", line 512, in <module>
    coll_df, coll_meta = pyreadstat.read_sas7bdat(COLL_FILE)
  File "pyreadstat/pyreadstat.pyx", line 129, in pyreadstat.pyreadstat.read_sas7bdat
  File "pyreadstat/_readstat_parser.pyx", line 1166, in pyreadstat._readstat_parser.run_conversion
  File "pyreadstat/_readstat_parser.pyx", line 908, in pyreadstat._readstat_parser.run_readstat_parser
  File "pyreadstat/_readstat_parser.pyx", line 830, in pyreadstat._readstat_parser.check_exit_status
pyreadstat._readstat_parser.ReadstatError: Invalid file, or file has unsupported features



COLL_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE = Path(f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831") and coll and desc are flat files not sas7bdat
