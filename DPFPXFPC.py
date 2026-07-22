IMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
Report Date: 21/07/2026
Week: 4
RDATE: 210726
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 663,747
  LOANTEMP after filtering: 387,612 rows
  Reading lnnote.sas7bdat (chunked, filtered as it streams)...
    ...scanned 6,232,608 raw rows total          
  LNNOTE after filtering: 1,150,348 rows
  ACCTNO dtype  -> lnnote: Float64, loantemp: Float64
  NOTENO dtype  -> lnnote: Float64, loantemp: Float64
  ACCTNO sample -> lnnote: [2000450411.0, 2013587108.0, 2016265320.0], loantemp: [8709015015.0, 8826241403.0, 8862941712.0]
  NOTENO sample -> lnnote: [90010.0, 90010.0, 90011.0], loantemp: [90010.0, 90010.0, 94010.0]
  Merging data...
  [diag] lnnote ACCTNO range:   min=2000450411, max=8996247905, unique=1,150,345
  [diag] loantemp ACCTNO range: min=2902719229, max=8996925404, unique=387,606
  [diag] ACCTNO overlap (ignoring NOTENO): 0 accounts in common
  [diag] ZERO ACCTNO overlap -> the two files do not share any account keys at all. This points to the files being from different sources, snapshot dates, or environments (e.g. test/dummy data), rather than a code/dtype problem. Verify LOAN_DIR and CCDTEMP_DIR point at the correct, matching production extracts for this report date.
  HP Loans after merge: 0 accounts
  ERROR: No matching records after merge
