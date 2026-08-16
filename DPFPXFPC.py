============================================================
EIBDLCRM - BNM LCR Reporting (Conventional Banking)
============================================================

NOTE: KALMLIQ logic integrated directly
      - Reading from BNMK.K1TBL{mon}{week} and BNMK.K3TBL{mon}{week}
      - Using hardcoded FX rates
============================================================

Report Date: 31/07/2026
Week: 4, Month: 07
Expected K1/K3 files: k1tbl074.sas7bdat
Expected UTSAS files: utms260731.sas7bdat, utfx260731.sas7bdat, utrp260731.sas7bdat

============================================================
LOADING INPUTS
============================================================

1. FX Rates (HARDCODED)...
  Loaded 10 currencies: ['MYR', 'USD', 'SGD', 'HKD', 'AUD', 'JPY', 'XAU', 'GBP', 'EUR', 'CNY']

2. Loading WALK.TXT and TEMPL.TXT...
    Warning: Could not read /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/list/walk.txt: invalid literal for int() with base 10: '1F144611FXS'
    Read 70 records from templ.txt
  WALK: 3 records
  TEMPL: 70 records

3. Processing KALMLIQ (K1TBL and K3TBL)...
  Looking for K1TBL file...
    Base path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/
    Month: 07, Week: 4
    Looking for exact matches:
      k1tbl074.sas7bdat: ✓ Found
  Using K1TBL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/k1tbl074.sas7bdat
    Successfully read: k1tbl074.sas7bdat (7199 rows, 61 columns)
  Processing K1TBL with 7199 rows...
    Columns: ['REPTDATE', 'GWAB', 'GWAN', 'GWAS', 'GWAPP', 'GWACS', 'GWBALA', 'GWBALC', 'GWPAIA', 'GWPAIC']...
    Unique values in GWMVT: ['P', '']
    Rows with GWMVT = 'P': 7198
    Sample rows (first 3):
      Row 1:
      Row 2:
      Row 3:
  K1TBL processing stats:
    Total rows: 7199
    Filtered out (GWMVT != 'P'): 1
    Passed GWMVT = 'P': 7198
    Excluded (XAU/XAT currency): 0
    Records with item assigned: 0
  K1TBL records: 0
  Looking for K3TBL file...
    Base path: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/
    Month: 07, Week: 4
    Looking for exact matches:
      k3tbl074.sas7bdat: ✓ Found
  Using K3TBL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/bnmk/k3tbl074.sas7bdat
    Successfully read: k3tbl074.sas7bdat (20171 rows, 39 columns)
  Processing K3TBL with 20171 rows...
    Columns: ['REPTDATE', 'UTSTY', 'UTREF', 'UTDLP', 'UTDLR', 'UTSMN', 'UTCUS', 'UTCLC', 'UTCTP', 'UTFCV']...
    Unique values in UTREF: ['ISV', '', 'DLG', 'AFS', 'INV', 'DRI', 'AFSLIQ', 'PSD']
    Unique values in UTSTY: ['MTB', 'ISB', 'MGS', 'ISD', 'DBD', 'DIM', 'LDC', 'PBA', 'ITB', 'CB1', 'DIC', 'MGI', '']
    Sample rows (first 3):
      Row 1:
      Row 2:
      Row 3:
  K3TBL processing stats:
    Total rows: 20171
    Rows matching UTREF patterns: 20148
    Records with item assigned: 1498
  K3TBL records: 1,498
  Total treasury records: 0

4. Processing DCIWH.DCID...
  Using DCI file: dcid0731.sas7bdat
    Successfully read: dcid0731.sas7bdat (230 rows, 33 columns)
    Columns: ['TICKETNO', 'CUSTNAME', 'NEWIC', 'SALESID', 'CUSTCODE', 'INVCURRAC', 'ALTCURRAC', 'ACCINT', 'ROLLOVER', 'CONVERTIND']...
    Sample rows (first 3):
      Row 1:
      Row 2:
      Row 3:
  DCI records: 0

5. Processing CIS.CUSTDLY (parquet)...
  Using CIS file: CIS_CUST_DAILY.parquet
    Successfully read: CIS_CUST_DAILY.parquet (33049519 rows, 99 columns)
  CIS equity warning: unable to find column "acctcode"; valid columns: ["CUSTNO", "BANKNO", "ACCTNOC", "ACCTNO", "ACCTCODE", "RLENCODE", "PRISEC", "RLENTYPE", "RLENDESC", "PROCESSTIME", "GENDER", "CUSTSTAT", "TAXCODE", "TAXID", "CUSTBRCH", "COSTCTR", "CUSTLASTDATECC", "CUSTLASTDATEYY", "CUSTLASTDATEMM", "CUSTLASTDATEDD", "CUSTLASTOPER", "PRIM_OFF", "SEC_OFF", "PRIM_LN_OFF", "SEC_LN_OFF", "RESIDENCY", "RACE", "CITIZENSHIP", "CUSTSINCEDATE", "CUSTOPENDATE", "HRC01", "HRC02", "HRC03", "HRC04", "HRC05", "HRC06", "HRC07", "HRC08", "HRC09", "HRC10", "HRC11", "HRC12", "HRC13", "HRC14", "HRC15", "HRC16", "HRC17", "HRC18", "HRC19", "HRC20", "EXPERIENCE", "HOBBIES", "RELIGION", "LANGUAGE", "INST_SEC", "CUST_CODE", "CUSTCONSENT", "BASICGRPCODE", "MSICCODE", "MASCO2008", "CUSTMNTDATE", "INDORG", "HRCINDC", "HRC998", "HRCPEP", "HRC037", "ADDREF", "CUSTNAME", "PRIPHONE", "SECPHONE", "MOBILEPH", "FAX", "NAMEFMT", "ALIASKEY", "ALIAS", "BNMKEY", "BNMID", "INCOME", "EDUCATION", "OCCUP", "MARITALSTAT", "OWNRENT", "EMPNAME", "DOBCC", "DOBYY", "DOBMM", "DOBDD", "DOBDOR", "SICCODE", "CORPSTATUS", "NETWORTH", "LONGNAME", "JOINTACC", "PRCOUNTRY", "EMPLNAME", "EMPLTYPE", "EMPLSECT", "EMPLDATE", "EMPLTIME"]
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDLCRM.py", line 1059, in process_cis_equity
    df = df.filter((pl.col('acctcode') == 'EQC') & (pl.col('prisec') == 901))
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 5325, in filter
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.ColumnNotFoundError: unable to find column "acctcode"; valid columns: ["CUSTNO", "BANKNO", "ACCTNOC", "ACCTNO", "ACCTCODE", "RLENCODE", "PRISEC", "RLENTYPE", "RLENDESC", "PROCESSTIME", "GENDER", "CUSTSTAT", "TAXCODE", "TAXID", "CUSTBRCH", "COSTCTR", "CUSTLASTDATECC", "CUSTLASTDATEYY", "CUSTLASTDATEMM", "CUSTLASTDATEDD", "CUSTLASTOPER", "PRIM_OFF", "SEC_OFF", "PRIM_LN_OFF", "SEC_LN_OFF", "RESIDENCY", "RACE", "CITIZENSHIP", "CUSTSINCEDATE", "CUSTOPENDATE", "HRC01", "HRC02", "HRC03", "HRC04", "HRC05", "HRC06", "HRC07", "HRC08", "HRC09", "HRC10", "HRC11", "HRC12", "HRC13", "HRC14", "HRC15", "HRC16", "HRC17", "HRC18", "HRC19", "HRC20", "EXPERIENCE", "HOBBIES", "RELIGION", "LANGUAGE", "INST_SEC", "CUST_CODE", "CUSTCONSENT", "BASICGRPCODE", "MSICCODE", "MASCO2008", "CUSTMNTDATE", "INDORG", "HRCINDC", "HRC998", "HRCPEP", "HRC037", "ADDREF", "CUSTNAME", "PRIPHONE", "SECPHONE", "MOBILEPH", "FAX", "NAMEFMT", "ALIASKEY", "ALIAS", "BNMKEY", "BNMID", "INCOME", "EDUCATION", "OCCUP", "MARITALSTAT", "OWNRENT", "EMPNAME", "DOBCC", "DOBYY", "DOBMM", "DOBDD", "DOBDOR", "SICCODE", "CORPSTATUS", "NETWORTH", "LONGNAME", "JOINTACC", "PRCOUNTRY", "EMPLNAME", "EMPLTYPE", "EMPLSECT", "EMPLDATE", "EMPLTIME"]
  CIS records: 0

6. Processing EQUA.UTMS/UTFX/UTRP...
  Looking for UTSAS files in: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/equa/
    RPTDT format: 260731 (260731)
    Total files in directory: 3
    First 20 files:
      - utfx260731.sas7bdat
      - utms260731.sas7bdat
      - utrp260731.sas7bdat

    Looking for utms files...
      Pattern 'utms260731.sas7bdat' matched 1 file(s):
        - utms260731.sas7bdat
      Using: utms260731.sas7bdat
    Successfully read: utms260731.sas7bdat (20096 rows, 99 columns)
      Columns in utms260731.sas7bdat: ['BRANCH', 'DEALREF', 'DEALTYPE', 'PORTREF', 'DEPOTID', 'DEPOTFD', 'CUSTNO', 'CUSTLOC', 'CUSTNAME', 'CUSTEQNO', 'CUSTEQTP', 'CUSTACC', 'CUSTID', 'CUSTFISS', 'SECNO', 'CURRENCY', 'SECTYPE', 'SECDESC', 'SECISSR', 'ISSRLOC', 'ISSRNAME', 'ISSRACPN', 'ISSREQTP', 'ISSRACC', 'ISSRID', 'ISSRTP2', 'COUPONRT', 'CAPVALUE', 'DEALDESC', 'FALTVALU', 'FACEVALU', 'CAPPRICE', 'DISCQUOT', 'SALEDP', 'TRANNO', 'TRANTYPE', 'QTYLDMAT', 'CUSTANAL', 'CUSTSANA', 'CUSTRESC', 'CUSTPRTC', 'CUSTRSKC', 'ISSRPRAT', 'ISSRARAT', 'DEALRCD', 'BROKRCD', 'BROKAMT', 'SUNDREF', 'INTACR', 'ACPRFT', 'ACPRFTYS', 'AMTACRIN', 'CAMTOWNE', 'AMTOWNED', 'BOOKVALU', 'DISCPRM', 'MRKTVALU', 'DISCPLTD', 'DISCPRTD', 'UNDISCPR', 'DISCPRMN', 'DISCPRPF', 'ORGDISC', 'YIELDMAT', 'PORTTYPE', 'BASERATE', 'CAPFLAT', 'COUPFREQ', 'COUPRATE', 'COUPSPRE', 'ISSRSUND', 'ISSRRESC', 'ISSRPRTC', 'ISSRACPT', 'INTBEAR', 'ISSPRICE', 'ISSSIZE', 'PRICEIND', 'SECLEGCD', 'SECORGCD', 'MRKTMKR1', 'STDSECDC', 'RACE', 'BUSDATE', 'TRDDATE', 'VALUDATE', 'MATDATE', 'ISSDATE', 'LSTCPNDT', 'NXTCPNDT', 'CSTLC', 'CLTLC', 'DEAL_FOLDER', 'REVAL_ACCRUED_AMT', 'TYPE_OF_DEAL', 'FO_DEAL_ID', 'UNREALISE_PROFIT_LOSS', 'EXP_CREDIT_LOSS', 'CAPITAL_INSTRUMENT_TYPE']

    Looking for utfx files...
      Pattern 'utfx260731.sas7bdat' matched 1 file(s):
        - utfx260731.sas7bdat
      Using: utfx260731.sas7bdat
    Successfully read: utfx260731.sas7bdat (6061 rows, 39 columns)
      Columns in utfx260731.sas7bdat: ['BRANCH', 'DEALTYPE', 'DEALREF', 'APPLCODE', 'BASICTYP', 'CUSTNO', 'CUSTLOC', 'CUSTNAME', 'CUSTEQNO', 'CUSTEQTP', 'CUSTACC', 'CUSTID', 'CUSTFISS', 'MOVETYPE', 'MOVESUB', 'PURCHCUR', 'SALESCUR', 'AMTPAY', 'MMPRIAMT', 'AMTRECEI', 'EXCHRATE', 'CUSTANAL', 'CUSTSANA', 'CUSTRESC', 'CUSTPRTC', 'CUSTRSKC', 'CUSTGRP', 'DEALCODE', 'BROKRCD', 'BROKAMT', 'INTLSNBD', 'TOTINT', 'OPTDEAL', 'CUSTPART', 'RACE', 'MAYEQAMT', 'BUSDATE', 'STRTDATE', 'MATDATE']

    Looking for utrp files...
      Pattern 'utrp260731.sas7bdat' matched 1 file(s):
        - utrp260731.sas7bdat
      Using: utrp260731.sas7bdat
    Successfully read: utrp260731.sas7bdat (66 rows, 79 columns)
      Columns in utrp260731.sas7bdat: ['BRANCH', 'DEALTYPE', 'DEALREF', 'PORTREF', 'DEPOTID', 'CUSTNO', 'CUSTLOC', 'CUSTNAME', 'CUSTEQNO', 'CUSTEQTP', 'CUSTACC', 'CUSTID', 'CUSTFISS', 'SECNO', 'CURRENCY', 'SECTYPE', 'SECDESC', 'SECISSR', 'ISSRLOC', 'ISSRNAME', 'ISSRACPN', 'ISSREQTP', 'ISSRACC', 'ISSRID', 'ISSRTP2', 'PCHPRIC', 'SALEPRIC', 'CERNVALU', 'CERTPCHP', 'CERSALEP', 'TRANTYPE', 'FACEVALU', 'TPCHPROC', 'TSALPROC', 'RPINTRAT', 'CUSTANAL', 'CUSTSANA', 'CUSTRESC', 'CUSTPRTC', 'CUSTRSKC', 'BROKRCD', 'BROKAMT', 'PORTTYPE', 'ORIGPORT', 'BASERATE', 'CAPFLAT', 'COUPFREQ', 'COUPRATE', 'COUPSPRE', 'ISSRANAL', 'ISSRSUND', 'ISSRRESC', 'ISSRPRTC', 'ISSRACPT', 'ISSPRICE', 'SECLEGCD', 'SECORGCD', 'STDPOORS', 'MRKTMKR1', 'RREPREF', 'RRBRPAMT', 'RRSOSAMT', 'CUROWAMT', 'CURBOAMT', 'INTACRDT', 'CUSTPART', 'CUSTFUTU', 'INDPRC', 'INDYLD', 'BUSDATE', 'REPOSRTD', 'REPOMATD', 'DEALDATE', 'ISSDATE', 'MATDATE', 'NXTCPNDT', 'LSTCPNDT', 'TPCHPROC_FCY', 'TSALPROC_FCY']

    Total UTSAS records: 0
  UTSAS records: 0

7. Processing LCR.FD/SA/CA/FCYCA...
    Successfully read: fdhold.sas7bdat (57 rows, 8 columns)
    Successfully read: fd30.sas7bdat (2656534 rows, 11 columns)
    Successfully read: sa30.sas7bdat (4238423 rows, 7 columns)
    Successfully read: ca30.sas7bdat (825297 rows, 9 columns)
    Successfully read: fcyca30.sas7bdat (71165 rows, 9 columns)
  Banking records: 7,791,476

8. Processing CISDP/CISCA.DEPOSIT...
  CIS info records: 0

9. Processing LIST.LCR_ECP...
    Successfully read: lcr_ecp.sas7bdat (92893 rows, 8 columns)
  ECP records: 0

============================================================
PROCESSING DATA
============================================================

Combined treasury + DCI: 0 records
Enhanced treasury: 0 records
Enhanced banking: 7,791,476 records

Applying insurance split...
Banking after insurance split: 7,791,476 records

Total records before consolidation: 7,791,476

Consolidating...
  Consolidated to 1 BNM code x currency combinations

Generating LCR report (text format)...
  ✓ lcr31.txt: 1 items x 1 columns

============================================================
SUMMARY
============================================================

Total: RM 0K

By Source:
  banking_fcyca: RM 0K
  banking_ca: RM 0K
  banking_sa: RM 0K
  banking_fd: RM 0K

============================================================
✓ EIBDLCRM Complete
============================================================


why are some of the records and datasets become 0 records? is it because of filters? or wrong datasets?
