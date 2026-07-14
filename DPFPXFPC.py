PBBDPFMT module loaded successfully
Available functions: ['CADenomFormat', 'CAProductFormat', 'Dict', 'FCYTermFormat', 'FDDenomFormat', 'FDProductFormat', 'List', 'Optional', 'ProductLists', 'SADenomFormat', 'SAProductFormat', 'Set', 'Tuple', 'apply_format', 'branchcd_format', 'cadenom_format', 'caprod_format', 'ddcustcd_format', 'ddrange_format', 'dpcustcd_format', 'fcyterm_format', 'fdcustcd_format', 'fddenom_format', 'fdorgmt_format', 'fdprd_format', 'fdprod_format', 'fdprodd_format', 'fdrmmt_format', 'get_format', 'ifdcuscd_format', 'race_format', 'rmfdorgmt_format', 's1range_format', 's2range_format', 'sacustcd_format', 'sadenom_format', 'saprod_format', 'sdrange_format', 'statecd_format']
Processing report date...
Report Date: 13072026, Week: 4
Processing FDMTHLY data...
Read 2756145 records from FDMTHLY
SAS metadata: 22 columns, 2756145 rows
Calculating REMMTH...
Summarizing data...
Creating BNM codes...

Processing PBBDPFMT program...
PBBDPFMT module contains format functions but no data retrieval functions
Available functions: ['CADenomFormat', 'CAProductFormat', 'Dict', 'FCYTermFormat', 'FDDenomFormat', 'FDProductFormat', 'List', 'Optional', 'ProductLists', 'SADenomFormat', 'SAProductFormat', 'Set', 'Tuple', 'apply_format', 'branchcd_format', 'cadenom_format', 'caprod_format', 'ddcustcd_format', 'ddrange_format', 'dpcustcd_format', 'fcyterm_format', 'fdcustcd_format', 'fddenom_format', 'fdorgmt_format', 'fdprd_format', 'fdprod_format', 'fdprodd_format', 'fdrmmt_format', 'get_format', 'ifdcuscd_format', 'race_format', 'rmfdorgmt_format', 's1range_format', 's2range_format', 'sacustcd_format', 'sadenom_format', 'saprod_format', 'sdrange_format', 'statecd_format']
Available data variables: ['ACE', 'ACE_PRODUCTS', 'CURX', 'CURX_PRODUCTS', 'FCY', 'FCY_PRODUCTS']

Generating reports...
Generating report for 42130...
Report saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQSPEC/SPECIAL_REPORT_42130_13072026.txt
Generating report for 42132...
No data for 42132
Generating FCY FD report for 42630...
Report saved: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQSPEC/FCY_FD_REPORT_42630_13072026.txt

Saving processed data...

All files saved successfully!

============================================================
PROCESSING SUMMARY
============================================================
Report Date: 13/07/2026
Week: 4, Month: 07, Year: 2026
Total FDMTHLY records processed: 2,756,145
Total ALM records: 2,756,145
Total ALMDEPT records: 1,309

AMOUNT SUMMARY BY BIC PREFIX:
----------------------------------------
  42130:     3,258,867,152.31
  42132:                 0.00
  42630:     2,573,519,633.06
----------------------------------------
  GRAND TOTAL:     5,832,386,785.37

DISTRIBUTION BY CUSTOMER CODE CATEGORY:
----------------------------------------
  Codes 81-84:           558,847.68
  Codes 85+  :     5,831,827,937.69

DISTRIBUTION BY MATURITY BUCKET:
----------------------------------------
  NEGATIVE            4,221,855,247.42
  0-1 MONTH             473,625,734.63
  1-2 MONTHS            271,235,352.48
  2-3 MONTHS            105,060,729.06
  >12 MONTHS              3,677,059.72
  3-4 MONTHS            315,194,986.57
  4-5 MONTHS            242,144,671.64
  5-6 MONTHS            199,013,993.91
  6-7 MONTHS                252,000.00
  7-8 MONTHS                 10,000.00
  8-9 MONTHS                  7,009.94
  11-12 MONTHS              310,000.00

============================================================
REPORT GENERATION SUMMARY
============================================================
42130 Special Report: Total =     3,258,867,152.31
42132 Special Report: No data
42630 FCY FD Report: Total =     2,573,519,633.06

============================================================
PROCESSING COMPLETE
============================================================
You have mail in /var/spool/mail/sas_edw_dev
