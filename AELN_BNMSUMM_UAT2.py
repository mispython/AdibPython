===========================================================
EIBMRNID SAS7BDAT PROCESSOR
============================================================
Report Date: 30/11/2025
NID File: /stgsrcsys/host/uat/rnidm09.sas7bdat

📂 Reading SAS file...
✅ Loaded: 580 records

📋 Original columns (64):
    1. NID_ACCTNO
    2. NID_REFNO
    3. ACCTNO
    4. NID_CDNO
    5. CUSTNAME
    6. NEWIC
    7. BRANCH
    8. CUSTCODE
    9. TRANCHENO
   10. STARTDT
   11. MATDT
   12. INTRATE
   13. INTPLTDRATE
   14. CURBAL
   15. LSTINTPAYDT
   16. NXTINTPAYDT
   17. INTFRQ
   18. NIDSTAT
   19. ACCINT
   20. CDSTAT
   21. POSTSTAT
   22. EARLY_WDDT
   23. EARLY_WDPROC
   24. EARLY_TELRID
   25. EARLY_OFFID1
   26. EARLY_OFFID2
   27. CANCDT
   28. CANC_TELRID
   29. CANC_OFFID1
   30. CANC_OFFID2
   31. APPLDT
   32. APPLDTTM
   33. APPL_TELRID
   34. APPL_OFFID1
   35. APPL_OFFID2
   36. PRINTDT
   37. PRINT_TELRID
   38. PRINT_OFFID1
   39. PRINT_OFFID2
   40. MAINTDT
   41. MAINT_TELRID
   42. MAINT_OFFID1
   43. MAINT_OFFID2
   44. PLEDGESTAT
   45. NID_ODACCTNO
   46. CCOLLNO
   47. PLEDGEDT
   48. PLEDGE_TELRID
   49. PLEDGE_OFFID1
   50. PLEDGE_OFFID2
   51. CPLEDGEDT
   52. CPLEDGE_TELRID
   53. CPLEDGE_OFFID1
   54. CPLEDGE_OFFID2
   55. TERM
   56. SUITABILITY_ASSESSMENT_CD
   57. SALES_STAFF_ID
   58. TERMID
   59. INTPD
   60. COSTCTR
   61. PRODUCT
   62. BRABBR
   63. CURCODE
   64. CUSTCD

🔧 Checking for duplicate columns...

🔧 Standardizing column names...
✅ Will rename 7 columns:
  TRANCHENO → trancheno
  STARTDT → startdt
  MATDT → matdt
  CURBAL → curbal
  NIDSTAT → nidstat
  CDSTAT → cdstat
  EARLY_WDDT → early_wddt

📋 Final columns (64):
    1. NID_ACCTNO                     Float64
    2. NID_REFNO                      String
    3. ACCTNO                         Float64
    4. NID_CDNO                       String
    5. CUSTNAME                       String
    6. NEWIC                          String
    7. BRANCH                         Float64
    8. CUSTCODE                       Float64
    9. trancheno                      String
   10. startdt                        Float64
   11. matdt                          Float64
   12. INTRATE                        Float64
   13. INTPLTDRATE                    Float64
   14. curbal                         Float64
   15. LSTINTPAYDT                    Float64
   16. NXTINTPAYDT                    Float64
   17. INTFRQ                         String
   18. nidstat                        String
   19. ACCINT                         Float64
   20. cdstat                         String
   21. POSTSTAT                       String
   22. early_wddt                     Float64
   23. EARLY_WDPROC                   Float64
   24. EARLY_TELRID                   String
   25. EARLY_OFFID1                   String
   26. EARLY_OFFID2                   String
   27. CANCDT                         Float64
   28. CANC_TELRID                    String
   29. CANC_OFFID1                    String
   30. CANC_OFFID2                    String
   31. APPLDT                         Float64
   32. APPLDTTM                       String
   33. APPL_TELRID                    String
   34. APPL_OFFID1                    String
   35. APPL_OFFID2                    String
   36. PRINTDT                        Float64
   37. PRINT_TELRID                   String
   38. PRINT_OFFID1                   String
   39. PRINT_OFFID2                   String
   40. MAINTDT                        Float64
   41. MAINT_TELRID                   String
   42. MAINT_OFFID1                   String
   43. MAINT_OFFID2                   String
   44. PLEDGESTAT                     String
   45. NID_ODACCTNO                   Float64
   46. CCOLLNO                        Float64
   47. PLEDGEDT                       Float64
   48. PLEDGE_TELRID                  String
   49. PLEDGE_OFFID1                  String
   50. PLEDGE_OFFID2                  String
   51. CPLEDGEDT                      Float64
   52. CPLEDGE_TELRID                 String
   53. CPLEDGE_OFFID1                 String
   54. CPLEDGE_OFFID2                 String
   55. TERM                           Float64
   56. SUITABILITY_ASSESSMENT_CD      String
   57. SALES_STAFF_ID                 String
   58. TERMID                         String
   59. INTPD                          Float64
   60. COSTCTR                        Float64
   61. PRODUCT                        Float64
   62. BRABBR                         String
   63. CURCODE                        String
   64. CUSTCD                         String

📅 Converting SAS dates...
  Converted matdt (numeric → date)
  Converted startdt (numeric → date)
  Converted early_wddt (numeric → date)

💰 Positive balance filter: 580 → 580 records

🧮 Calculating remaining months...
✅ Remaining months calculated

🎯 Applying SAS formats...

📊 Creating Table 1...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 394, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBMRNID_NID.py", line 267, in main
    tbl1 = df.filter(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 10020, in with_columns
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 330, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2335, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "heldmkt"; valid columns: ["NID_ACCTNO", "NID_REFNO", "ACCTNO", "NID_CDNO", "CUSTNAME", "NEWIC", "BRANCH", "CUSTCODE", "trancheno", "startdt", "matdt", "INTRATE", "INTPLTDRATE", "curbal", "LSTINTPAYDT", "NXTINTPAYDT", "INTFRQ", "nidstat", "ACCINT", "cdstat", "POSTSTAT", "early_wddt", "EARLY_WDPROC", "EARLY_TELRID", "EARLY_OFFID1", "EARLY_OFFID2", "CANCDT", "CANC_TELRID", "CANC_OFFID1", "CANC_OFFID2", "APPLDT", "APPLDTTM", "APPL_TELRID", "APPL_OFFID1", "APPL_OFFID2", "PRINTDT", "PRINT_TELRID", "PRINT_OFFID1", "PRINT_OFFID2", "MAINTDT", "MAINT_TELRID", "MAINT_OFFID1", "MAINT_OFFID2", "PLEDGESTAT", "NID_ODACCTNO", "CCOLLNO", "PLEDGEDT", "PLEDGE_TELRID", "PLEDGE_OFFID1", "PLEDGE_OFFID2", "CPLEDGEDT", "CPLEDGE_TELRID", "CPLEDGE_OFFID1", "CPLEDGE_OFFID2", "TERM", "SUITABILITY_ASSESSMENT_CD", "SALES_STAFF_ID", "TERMID", "INTPD", "COSTCTR", "PRODUCT", "BRABBR", "CURCODE", "CUSTCD", "reptdate", "startdte", "remmth"]
