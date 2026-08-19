============================================================
EIIQINST - Islamic Trustee and Client Account Reporting
============================================================

Report Period: 12/2025 (Week: 4)
SDESC: PUBLIC BANK BERHAD

============================================================
INPUT FILES
============================================================

PIDMS directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

SACA directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

DEPOSIT directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

UNCLAIM directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

Processing Trustee Accounts...
  FLOAT: 18927 rows
  IBGPIDM: 7609 rows
  REMIT: 6385 rows
  SA/CA/FD: 9 rows
  Trustee >60k: 0 accounts
  Trustee <=60k: 1 accounts
  Output written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/islamic_trustee_low.txt

TRUSTEE <=60000 by Branch:
  Branch 161.0: RM 18,305.23

Processing Client Accounts...
  Found CLIENT file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/client.sas7bdat
  CLIENT master: 617 rows
  CLIENT columns: ['acctno', 'name', 'branch', 'product', 'curbal', 'intpaybl', 'float', 'avbal', 'avbaltt', 'prodcd', 'amtind', '_type_', '_freq_', 'plusbal', 'unclaim', 'cheqno', 'issbranch', 'status', 'issdte', 'category', 'si', 'ibgamt', 'key']
  SASA: 1 rows
  Deposit (from saca) columns: ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
  Deposit after FLOAT merge columns: ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl', 'float']
  Deposit after AVBAL calculation columns: ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl', 'float', 'avbal', 'avbaltt']
  Client after merge with deposit: 0 rows
  Client columns after merge: ['acctno', 'name', 'branch', 'product', 'curbal', 'intpaybl', 'float', 'avbal', 'avbaltt', 'prodcd', 'amtind', '_type_', '_freq_', 'plusbal', 'unclaim', 'cheqno', 'issbranch', 'status', 'issdte', 'category', 'si', 'ibgamt', 'key', 'branch_deposit', 'name_deposit', 'purpose', 'product_deposit', 'curbal_deposit', 'intpaybl_deposit', 'float_deposit', 'avbal_deposit', 'avbaltt_deposit']
  Final client columns: ['acctno', 'name', 'branch', 'product', 'curbal', 'intpaybl', 'float', 'avbal', 'avbaltt', 'prodcd', 'amtind_x', '_type_', '_freq_', 'plusbal_x', 'unclaim_x', 'cheqno', 'issbranch', 'status', 'issdte', 'category', 'si', 'ibgamt_x', 'key', 'branch_deposit', 'purpose', 'product_deposit', 'curbal_deposit', 'intpaybl_deposit', 'float_deposit', 'avbal_deposit', 'avbaltt_deposit', 'amtind_y', 'plusbal_y', 'unclaim_y', 'plusbal', 'unclaim', 'ibgamt_y', 'ibgamt']
  Client >60k: 0 accounts
  Client <=60k: 0 accounts

Checking for duplicate accounts...

============================================================
SUMMARY
============================================================

Trustee Accounts:
  Total: RM 18,305.23
  >60k: RM 0.00 (0 accounts)
  <=60k: RM 18,305.23 (1 accounts)

============================================================
✓ EIIQINST Complete
