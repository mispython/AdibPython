PBBDPFMT imported successfully
============================================================
EIBQINST - Trustee and Client Account Quarterly Reporting
============================================================

Report Date: 2026-07-31 (Week: 4)

Loading data...
  FLOAT: 18927 records
  IBGPIDM: 7609 records
  REMIT/UNCLAIM: 6385 records
  SA/CA/FD: 162 records
  DEP: 920763 records
  CLIENT: 3338 records

Debug - Client sample ACCTNOs: ['3222529634.0', '3227349032.0', '3214804534.0', '3136639312.0', '7048382012.0']
Debug - SACA sample ACCTNOs: ['4004361811.0', '4229557911.0', '4539277602.0', '4681614906.0', '5001255206.0']
Debug - Client ACCTNO type: String
Debug - SACA ACCTNO type: String

============================================================
Processing Trustee Accounts...
============================================================

Trustee >60k: 37 accounts
Trustee <=60k: 6 accounts

Writing Trustee output files...
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

============================================================
Processing Client Accounts...
============================================================
Debug - Client accounts: 3338
Debug - SACA accounts: 127
Debug - Overlap: 0
Debug - Client after join with SACA: 0

============================================================
Checking for duplicate accounts...
============================================================

============================================================
SUMMARY
============================================================

Trustee Accounts:
  Total: RM 22,747,258.98
  >60k: RM 22,574,809.67 (37 accounts)
  <=60k: RM 172,449.31 (6 accounts)

============================================================
✓ EIBQINST Complete
============================================================
