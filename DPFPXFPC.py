============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Processing Trustee Accounts...
  FLOAT: 18927 records loaded
  IBGPIDM: 7609 records loaded
    REMIT columns: ['acctno', 'cheqno', 'issbranch', 'ledgbal', 'status', 'paymode', 'name', 'issdte', 'category']
    UNCLAIM columns: ['paymode', 'ledgbal', 'acctno', 'status', 'name', 'category']
Error loading REMIT/UNCLAIM: type Float64 is incompatible with expected type Int32
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQINST.py", line 145, in load_remit
    combined = pl.concat([remit_subset, unclaim_subset])
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type Float64 is incompatible with expected type Int32
  REMIT/UNCLAIM: 0 records loaded
  SA columns: ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
  CA columns: ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
  FD columns: ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
  SA/CA/FD: 162 records loaded
    SA: 10, CA: 109, FD: 43
  DEP: 920763 records loaded
  Trustee >60k: 37 accounts
  Trustee <=60k: 6 accounts
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_high.txt
  Written to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQINST/trustee_low.txt

TRUSTEE >60000 by Branch:
  Branch 2.0: RM 6,034,191.79
  Branch 4.0: RM 1,614,488.67
  Branch 18.0: RM 938,255.52
  Branch 168.0: RM 3,752,676.82
  Branch 196.0: RM 10,235,196.87

TRUSTEE <=60000 by Branch:
  Branch 18.0: RM 105,384.65
  Branch 168.0: RM 39,798.43
  Branch 196.0: RM 27,266.23

Processing Client Accounts...
  CLIENT master: 3338 records loaded

Checking for duplicate accounts...

============================================================
SUMMARY
============================================================

Trustee Accounts:
  Total: RM 22,747,258.98
  >60k: RM 22,574,809.67 (37 accounts)
  <=60k: RM 172,449.31 (6 accounts)

============================================================
✓ EIBQINST Complete
