Report date: 31/08/2026
Input files:
  CURRENT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m08.sas7bdat
  LIMIT: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m08.sas7bdat
  COLL: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
  DESC: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831

Reading SAS datasets...
CURRENT dataset: 1132084 rows before filtering
CURRENT dataset: 967521 rows after filtering
LIMIT dataset: 1093140 rows before filtering
LIMIT dataset: 937196 rows after filtering
NPLA dataset: 28 rows

Reading CISDP dataset in chunks...
Processed 10 chunks, 1000000 rows total
Processed 20 chunks, 2000000 rows total
Processed 30 chunks, 3000000 rows total
Processed 40 chunks, 4000000 rows total
Processed 50 chunks, 5000000 rows total
Processed 60 chunks, 6000000 rows total
Processed 70 chunks, 7000000 rows total
Processed 80 chunks, 8000000 rows total
Processed 90 chunks, 9000000 rows total
Processed 100 chunks, 10000000 rows total
Processed 110 chunks, 10935758 rows total
CISDP dataset: 8666482 rows after filtering

CA after SCH mapping: 65 rows
Processing LIMIT data...
LIMIT processed: 930567 unique records
CA after LIMIT merge: 67 rows
CA after GP3 merge: 67 rows
CA after CISDP merge: 67 rows
DEP after MICR merge: 67 rows
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDNPGS.py:298: FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
  dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])

DEP after CVAR02 mapping: 67 rows

Writing output to DPNPGS_08.sas7bdat...
Final dataset: 67 rows
Columns: ['BRANCH', 'ACCTNO', 'NAME', 'TAXNO', 'DEBIT', 'CREDIT', 'CLOSEDT', 'REOPENDT', 'CUSTCODE', 'ODPLAN', 'RATE1', 'RATE2', 'RATE3', 'RATE4', 'RATE5', 'TODRATE', 'FLATRATE', 'BASERATE', 'ODSTAT', 'ORGCODE', 'ORGTYPE', 'LIMIT1', 'LIMIT2', 'LIMIT3', 'LIMIT4', 'LIMIT5', 'INTYTD', 'FEEPD', 'PURPOSE', 'COL1', 'COL2', 'COL3', 'COL4', 'COL5', 'SECTOR', 'USER2', 'USER3', 'RISKCODE', 'LEDGBAL', 'DATE_LST_DEP', 'OPENIND', 'STATCD', 'LASTTRAN', 'RETURNS_Y', 'CHGIND', 'AVGAMT', 'PRODUCT', 'RACE', 'DEPTYPE', 'INT1', 'INTPD', 'INTPLAN', 'CURBAL', 'CHQFLOAT', 'IA_LRU', 'MTDLOWBA', 'BENINTPD', 'STATE', 'INTCYCODE', 'APPRLIMT', 'ODINTCHR', 'ODINTACC', 'CURCODE', 'INTRATE', 'YTDAVAMT', 'BDATE', 'INACTIVE', 'SECOND', 'ODXSAMT', 'BONUTYPE', 'SERVICE', 'BONUSANO', 'USER5', 'TRACKCD', 'EXODDATE', 'TEMPODDT', 'PREVBRNO', 'AVGBAL', 'COSTCTR', 'AUTHORISE_LIMIT', 'CRRCODE', 'CCRICODE', 'FAACRR', 'POST_IND', 'CENSUST', 'ACCPROF', 'MAXPROF', 'INTRSTPD', 'MTDAVBAL', 'VB', 'BILLERIND', 'MODIFIED_FACILITY_IND', 'DTLSTCUST', 'INTPDPYR', 'OPENDT', 'MAILCODE', 'OMNILOAN', 'OMNITRDB', 'DSR', 'REPAY_TYPE_CD', 'MTD_REPAID_AMT', 'INDUSTRIAL_SECTOR_CD', 'MTD_DISBURSED_AMT', 'MTD_REPAY_TYPE10_AMT', 'MTD_REPAY_TYPE20_AMT', 'MTD_REPAY_TYPE30_AMT', 'RRIND', 'RRCOUNT', 'RRAPPRVDT', 'RREXPIRYDT', 'RRMAINDT', 'RRPERIOD', 'RRCOMPLDT', 'PB_ENTERPRISE_REG_DT', 'PSREASON', 'INSTRUCTIONS', 'WRITE_DOWN_BAL', 'PROP_DEVELOP_FIN_IND', 'NXT_STMT_CYCLE_DT', 'DPMTDBAL', 'INTPAYBL', 'OPENMH', 'CLOSEMH', 'ACCYTD', 'PB_ENTERPRISE_TAG', 'DNBFISME', 'FORATE', 'FORBAL', 'CURBALUS', 'PRIN_ACCT', 'STMT_CYCLE', 'ENTITY_CD', 'COV_OPT_OUT', 'INTPLAN_IBCA', 'PB_ENTERPRISE_PACKAGE_CD', 'L_DEP', 'E_INVOICE_IND', 'CASH_DEPOSIT_LIMIT_IND', 'SOURCE_INCOME_CURRENCY_CD', 'CASH_DEPOSIT_AMOUNT_AGG', 'POST_IND_MAINT_DT', 'POST_IND_EXP_DT', 'LAST_LIMIT_REV_DATE', 'NEXT_LIMIT_REV_DATE', 'FDB_TAG', 'FDB_TAG_DT', 'FDB_SCORING_DT', 'WRIOFF_CLOSE_FILE_TAG', 'WRIOFF_CLOSE_FILE_TAG_DT', 'SCH', 'LMTSTART', 'NPLDATE', 'NEWIC', 'CUSTNAME', 'MICRCD', 'ARREARS', 'NPLDATE_CALC', 'CVAR02', 'CVAR01', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07', 'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14', 'CVAR15', 'NDATE', 'STATUS']
Sample data:
   BRANCH        ACCTNO             NAME      TAXNO   DEBIT     CREDIT  CLOSEDT  REOPENDT  CUSTCODE  ODPLAN  RATE1  RATE2  ...      CVAR06  CVAR07    CVAR08  CVAR09      CVAR10  CVAR11 CVAR12 CVAR13 CVAR14  CVAR15  NDATE  STATUS
0   107.0  3.071098e+09  UA PAINTS & HAR  000000000  582.49  127503.94      NaN       NaN      47.0   101.0   7.97   7.97  ...  3071098223      OD  686000.0     0.0  1841836.54       0           NaN   0233  7072.0    NaN     NaN
1   107.0  3.071098e+09  UA PAINTS & HAR  000000000  582.49  127503.94      NaN       NaN      47.0   101.0   7.97   7.97  ...  3071098223      OD  686000.0     0.0  1841836.54       0           NaN   0233  7072.0    NaN     NaN
2   107.0  3.071098e+09  UA PAINTS & HAR  000000000  582.49  127503.94      NaN       NaN      47.0   101.0   7.97   7.97  ...  3071098223      OD  686000.0     0.0  1841836.54       0           NaN   0233  7072.0    NaN     NaN
3   113.0  3.071720e+09  CINHO ENTERPRIS               0.00       0.00      NaN       NaN      46.0   100.0   0.00   0.00  ...  3071719525      OD       0.0     0.0    28655.21       0           NaN   0233  6048.0    NaN     NaN
4   113.0  3.071740e+09  JIA EU DEVELOPM             815.51       0.00      NaN       NaN      46.0   101.0   4.75   0.00  ...  3071739802      OD  700000.0     0.0   191247.76       0           NaN   0233  6048.0    NaN     NaN

[5 rows x 174 columns]
SAS Connection established. Subprocess id is 612261

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 612261
Output written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS/DPNPGS_08.sas7bdat
Total records: 67
