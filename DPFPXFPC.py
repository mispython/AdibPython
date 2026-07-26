============================================================
EIMIR103 SAS to Python Conversion - NPL Report
============================================================

1. Setting report date (yesterday)...
   Report Date: 250726
   Report Date (display): 25/07/26

2. Loading loan data from loantemp.sas7bdat...
   Loaded 663747 loan records
   Columns: ['ACCTNO', 'NOTENO', 'CAP', 'NAME', 'LSTTRNCD', 'CURBAL', 'COLLDESC', 'CENSUS', 'ORGBAL', 'FEEDUE']...
   Total loans: 663747

3. Loading branch data from LKP_BRANCH...
   Warning: Could not parse LKP_BRANCH: read_csv() got an unexpected keyword argument 'skip_after_blank'
   Total branches: 0

4. Categorizing NPL loans...
   NPL candidates: 1858

5. Merging with branch data...
   No branch data available - using NPL data without branch codes
   Using 1858 NPL records without branch merge

6. Generating Report A (EIMAR103-A)...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR103.py:330: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.count().alias("NOACC")
   Appended to CCDTXT2: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR103/CCDTXT2
   Report A saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR103/EIMAR103-A.txt

7. Generating Report B (EIMAR103-B)...
   No records for Report B (all excluded)

8. Analyzing NPL characteristics...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR103.py:571: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  borstat_dist = cat_data.group_by("BORSTAT").agg(pl.count().alias("COUNT"))
   NPL analysis saved: 2 categories

9. Creating summary statistics...
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR103.py:739: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  pl.count().alias("ACCOUNT_COUNT"),

============================================================
CONVERSION COMPLETE
============================================================
Total loans processed: 663747
NPL accounts identified: 1858
NPL balance: 56,767,389.17
Categories: 2
Output saved to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR103
CCDTXT2 appended at: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR103/CCDTXT2


btw, output the CCDTXT2 into /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR_CCDTXT2 (it will append with existing CCDTXT2.txt output)
