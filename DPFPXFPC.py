============================================================
Bad Debt Write-Off List Generation
============================================================
Report Date: 20/07/2026
Week: 4, Previous Month: 06
*** TEST MODE: Limiting to 500000 rows ***
============================================================

STEP 1: DEBUG - Checking BORSTAT values...
  Reading: lnnote.sas7bdat - 1000 rows, 0.9s

  BORSTAT column info:
  Data type: String
  Unique values (first 1000 rows):
    'W' : 35 rows
    '' : 923 rows
    'X' : 39 rows
    'P' : 1 rows
    'F' : 2 rows
  No 'A' values found in sample

STEP 2: Reading NPLA data...
  Reading: lnnote.sas7bdat - 500000 rows, 4.5s

  Checking BORSTAT values in full dataset...
  BORSTAT distribution:
    'X' : 1636 rows
    'C' : 11 rows
    'Y' : 1 rows
    'P' : 44 rows
    'I' : 9 rows
    'K' : 63 rows
    '' : 493191 rows
    'M' : 1 rows
    'R' : 6 rows
    'F' : 169 rows
    'A' : 3 rows
    'D' : 1 rows
    'W' : 4862 rows
    '0' : 3 rows

  Exact match 'A': 3 rows
  After strip 'A': 3 rows

  Using stripped 'A' filtering...

  NPLA rows: 3

STEP 3: Reading IIS and SP data...
  Reading: iis.sas7bdat - 135725 rows, 0.4s
  Reading: sp2.sas7bdat - 135725 rows, 0.4s
  IIS rows: 135725
  SP rows: 135725


STEP 4: Combining NPL data...
  NPL combined rows: 135728

STEP 5: Reading CCRIS data...
  Looking for: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/icredmsubac0726.sas7bdat
  Reading: icredmsubac0726.sas7bdat - 500000 rows, 33.0s
  Found CCRIS file: icredmsubac0726.sas7bdat
  Columns in CCRIS file: ['micr', 'acctnum', 'NOTENO', 'COSTCTR', 'FISSPURP', 'CUSTFIX', 'SECTFIX', 'FICODE', 'micrn', 'MASTACC', 'DATE', 'BALANCE', 'MTHARR', 'INSTARR', 'UNDRAWN', 'ACSTATUS', 'DAYSARR', 'NOTECHAR', 'BRANCH', 'BNM_SUBMISSION_DATE', 'MTHINSTLMT', 'ACCAA', 'AA', 'SUBNO', 'AAGRADE', 'NOTEDBY', 'DTNOTED', 'PRIN_OUTS', 'INST_OUTS', 'OTHER_CHARGES', 'REVRPAYDT', 'SDIS_STATUS', 'O_BRANCH', 'O_MICR', 'CREATEDT', 'REVISED', 'IIA', 'IA', 'IA_PROV', 'EIR', 'PAIDIND', 'LSTTRNCD', 'DISBURSE', 'REPAID', 'DTACCTST', 'PD', 'LGD', 'LTPAYCHG_TAWIDH', 'LTPAYCHG_GHARAMAH', 'TYPE_OF_ESTIMATE', 'CARD_ACCT_ID', 'FACILITY', 'SYNDIC', 'SPECFUND', 'PURPOSE', 'FCONCEPT', 'RR', 'CUSTCODE', 'PRODUCT', 'BNMSECT', 'DNBFISME', 'FACCODE', 'ACCT_TURN_NPL_DT', 'STRUPCO_3YR', 'DSR', 'REFIN_FLG', 'OLD_FI_CD', 'OLD_MASTACC', 'OLD_SUBACC', 'REFIN_AMT', 'PROBABILITY_ORIGI', 'APPL_REFNO', 'LN_UTILISE_LOCAT_CD', 'OMNIACCT', 'WRIDOWN_BAL', 'RRCRTDT', 'RRMAINDT', 'COUNTER', 'CENSUS', 'REMARKS', 'LNPURP', 'LNUTI', 'LOCLNUTI', 'REPAYCOUNTER', 'REPAYREMARKS', 'RRFRSCOUNTER', 'RRFRSREMARKS', 'CA_CAT', 'CA_PROV', 'CCY', 'INTRATE', 'REBATE', 'ASTPRC', 'PRCTYPE', 'MATDATE', 'DISBDATE', 'PRISEC', 'SECTFISS', 'CUSTFISS', 'REPORTINDSEC', 'REPORTSECT', 'REPORTPURP', 'ORI_WOD', 'LATEST_WOD', 'RRCOUNTDTE', 'RRCOUNTER', 'REFNOTENO', 'REFDAYSARR', 'RRIND', 'LU_TOWN_CITY', 'LU_POSTCODE', 'LU_STATE_CD', 'LU_COUNTRY_CD', 'REPAY_ASSIST_RR_TAG', 'REPAY_ASSIST_EFF_DT', 'INDUSTRIAL_SECTOR_CD', 'RRSTARTDT', 'RRCOMPLETEDT', 'UNDIS_IND', 'CLIMATE_PRIN_TAXONOMY_CLASS', 'CLIMATE_PRIN_TAX_CLASS_CCRIS', 'ACCT_CREATE_PROJECT_IND', 'WRIOFF_CLOSE_FILE_TAG', 'WRIOFF_CLOSE_FILE_TAG_DT', 'WRIOFF_IND', 'CMMATURDT', 'SECTOR', 'EIR_ADJ', 'BAL_AFT_EIR']
  Warning: Could not find account number column in CCRIS data
  Available columns: ['micr', 'acctnum', 'NOTENO', 'COSTCTR', 'FISSPURP', 'CUSTFIX', 'SECTFIX', 'FICODE', 'micrn', 'MASTACC', 'DATE', 'BALANCE', 'MTHARR', 'INSTARR', 'UNDRAWN', 'ACSTATUS', 'DAYSARR', 'NOTECHAR', 'BRANCH', 'BNM_SUBMISSION_DATE', 'MTHINSTLMT', 'ACCAA', 'AA', 'SUBNO', 'AAGRADE', 'NOTEDBY', 'DTNOTED', 'PRIN_OUTS', 'INST_OUTS', 'OTHER_CHARGES', 'REVRPAYDT', 'SDIS_STATUS', 'O_BRANCH', 'O_MICR', 'CREATEDT', 'REVISED', 'IIA', 'IA', 'IA_PROV', 'EIR', 'PAIDIND', 'LSTTRNCD', 'DISBURSE', 'REPAID', 'DTACCTST', 'PD', 'LGD', 'LTPAYCHG_TAWIDH', 'LTPAYCHG_GHARAMAH', 'TYPE_OF_ESTIMATE', 'CARD_ACCT_ID', 'FACILITY', 'SYNDIC', 'SPECFUND', 'PURPOSE', 'FCONCEPT', 'RR', 'CUSTCODE', 'PRODUCT', 'BNMSECT', 'DNBFISME', 'FACCODE', 'ACCT_TURN_NPL_DT', 'STRUPCO_3YR', 'DSR', 'REFIN_FLG', 'OLD_FI_CD', 'OLD_MASTACC', 'OLD_SUBACC', 'REFIN_AMT', 'PROBABILITY_ORIGI', 'APPL_REFNO', 'LN_UTILISE_LOCAT_CD', 'OMNIACCT', 'WRIDOWN_BAL','RRCRTDT', 'RRMAINDT', 'COUNTER', 'CENSUS', 'REMARKS', 'LNPURP', 'LNUTI', 'LOCLNUTI', 'REPAYCOUNTER', 'REPAYREMARKS', 'RRFRSCOUNTER', 'RRFRSREMARKS', 'CA_CAT', 'CA_PROV', 'CCY', 'INTRATE', 'REBATE', 'ASTPRC', 'PRCTYPE', 'MATDATE','DISBDATE', 'PRISEC', 'SECTFISS', 'CUSTFISS', 'REPORTINDSEC', 'REPORTSECT', 'REPORTPURP', 'ORI_WOD', 'LATEST_WOD', 'RRCOUNTDTE', 'RRCOUNTER', 'REFNOTENO', 'REFDAYSARR', 'RRIND', 'LU_TOWN_CITY', 'LU_POSTCODE', 'LU_STATE_CD', 'LU_COUNTRY_CD', 'REPAY_ASSIST_RR_TAG', 'REPAY_ASSIST_EFF_DT', 'INDUSTRIAL_SECTOR_CD', 'RRSTARTDT', 'RRCOMPLETEDT', 'UNDIS_IND', 'CLIMATE_PRIN_TAXONOMY_CLASS', 'CLIMATE_PRIN_TAX_CLASS_CCRIS', 'ACCT_CREATE_PROJECT_IND', 'WRIOFF_CLOSE_FILE_TAG', 'WRIOFF_CLOSE_FILE_TAG_DT', 'WRIOFF_IND', 'CMMATURDT', 'SECTOR', 'EIR_ADJ', 'BAL_AFT_EIR']
  CCRIS rows: 0

STEP 6: Reading HPD loan data...
  Reading: lnnote.sas7bdat - 500000 rows, 2.6s
  HPD loan rows: 2093

STEP 7: Merging data...
  Merged loan rows: 135728

STEP 8: Calculating derived fields...
  Reading: lnnote.sas7bdat - 500000 rows, 8.5s
  Warning: Could not calculate ECSRIND: unable to find column "Y"; valid columns: ["NAME", "ACCTNO", "NOTENO", "IIS", "OI", "TOTIIS", "SP", "MARKETVL", "BRANCH", "BRNO", "BRABBR", "DAYS", "FACILITY", "LOANTYPE", "GUAREND", "CUSTCODE", "LASTTRAN", "LSTTRNCD", "CURBAL", "INTAMT", "APPVALUE", "COLLDESC", "PAIDIND", "ORGBAL", "NETPROC", "MATUREDT", "BORSTAT", "INTEARN4", "PAYAMT", "FEETOTAL", "FEETOT2", "FEEAMT3", "POSTNTRN", "BIRTHDT", "SCORE2", "NFEEAMT5", "COLLYEAR", "DELQCD", "ECSRRSRV", "MODELDES", "CONTRTYPE", "AKPK_STATUS", "NACOSPADT", "CP", "FEEAMTA", "BALANCE", "FEEAMT5", "ISSXDTE", "POSTAMT", "OTHERAMT", "OIFEEAMT"]
  Warning: Could not calculate PAY75PCT: unable to find column "Y"; valid columns: ["NAME", "ACCTNO", "NOTENO", "IIS", "OI", "TOTIIS", "SP", "MARKETVL", "BRANCH", "BRNO", "BRABBR", "DAYS", "FACILITY", "LOANTYPE", "GUAREND", "CUSTCODE", "LASTTRAN", "LSTTRNCD", "CURBAL", "INTAMT", "APPVALUE", "COLLDESC", "PAIDIND", "ORGBAL", "NETPROC", "MATUREDT", "BORSTAT", "INTEARN4", "PAYAMT", "FEETOTAL", "FEETOT2", "FEEAMT3", "POSTNTRN", "BIRTHDT", "SCORE2", "NFEEAMT5", "COLLYEAR", "DELQCD", "ECSRRSRV", "MODELDES", "CONTRTYPE", "AKPK_STATUS", "NACOSPADT", "CP", "FEEAMTA", "BALANCE", "FEEAMT5", "ISSXDTE", "POSTAMT", "OTHERAMT", "OIFEEAMT", "ECSRIND", "BILPAID"]
  Calculations completed in 9.9s
  Loan records: 135728

STEP 9: Reading customer names...
  Reading: loan.sas7bdat
Error reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIFTXT1/loan.sas7bdat: Unable to read from file
  Customer names: 0

STEP 10: Reading guarantor information...
  Reading: lnliab07226.sas7bdat - 500000 rows, 0.8s
  Guarantor entries: 0

STEP 11: Reading previous balance...
  Reading: loan064.sas7bdat - 500000 rows, 0.8s
  SASLN rows: 20505

STEP 12: Final merge and filtering...
  WOFF before filter: 135728
  WOFF after filter: 0


No accounts identified for write-off

============================================================
COMPLETED IN 68.2 SECONDS
============================================================

Output files generated:
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftext.txt (Final formatted output)
  /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIFTXT1/wofftex1.txt (Intermediate output)


  why is it unable to read loan.sas7bdat
