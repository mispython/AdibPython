============================================================
EIBDLCRM - BNM LCR Reporting (Conventional Banking)
============================================================

NOTE: KALMLIQ logic integrated directly
      - Reading from BNMK.K1TBL{mon}{week} and BNMK.K3TBL{mon}{week}
      - Using hardcoded FX rates
      - Column names normalized to lowercase on read (fix applied)
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
    Read 63 records from walk.txt
    NOTE: LCRCDGL format lookup not populated - 'item' will be blank for all WALK records until ITEM_LOOKUP is filled in.
    Read 70 records from templ.txt
  WALK: 63 records
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
    Columns (61): ['reptdate', 'gwab', 'gwan', 'gwas', 'gwapp', 'gwacs', 'gwbala', 'gwbalc', 'gwpaia', 'gwpaic', 'gwshn', 'gwctp', 'gwact', 'gwacd', 'gwsac', 'gwnanc', 'gwcnal', 'gwccy', 'gwcnar', 'gwcnap', 'gwdiaa', 'gwdiac', 'gwciaa', 'gwciac', 'gwratd', 'gwratc', 'gwdipa', 'gwdipc', 'gwcipa', 'gwcipc', 'gwpl1d', 'gwpl2d', 'gwpl1c', 'gwpl2c', 'gwpala', 'gwpalc', 'gwdlp', 'gwdlr', 'gwsdt', 'gwrdt', 'gwrrt', 'gwpdt', 'gwprt', 'gwpcm', 'gwmotc', 'gwmrtc', 'gwmrt', 'gwmdt', 'gwmcm', 'gwmwm', 'gwmvt', 'gwmvts', 'gwsrc', 'gwuc1', 'gwuc2', 'gwc2r', 'gwamap', 'gwexr', 'gwopt', 'gwocy', 'gwcbd']
    !! WARNING [K1TBL]: expected columns not found after normalization: ['gwmpts', 'gwhsn']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    Unique values in GWMVT: ['P', '']
    Rows with GWMVT = 'P': 7198
    Sample rows (first 3):
      Row 1:
        gwmvt: 
        gwccy: 
        gwocy: 
        gwctp: 
        gwdlp: 
        gwmdt: None
        gwbalc: None
      Row 2:
        gwmvt: P
        gwccy: USD
        gwocy: MYR
        gwctp: CD
        gwdlp: FXS
        gwmdt: 24321.0
        gwbalc: 408600.0
      Row 3:
        gwmvt: P
        gwccy: USD
        gwocy: CNY
        gwctp: BA
        gwdlp: FXS
        gwmdt: 24321.0
        gwbalc: -4086000.0
  K1TBL processing stats:
    Total rows: 7199
    Filtered out (GWMVT != 'P'): 1
    Passed GWMVT = 'P': 7198
    Excluded (XAU/XAT currency): 14
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
    Columns (39): ['reptdate', 'utsty', 'utref', 'utdlp', 'utdlr', 'utsmn', 'utcus', 'utclc', 'utctp', 'utfcv', 'utidt', 'utlcd', 'utncd', 'utmdt', 'utcbd', 'utcpr', 'utqds', 'utpcp', 'utamoc', 'utdpf', 'utaict', 'utaicy', 'utait', 'utdpet', 'utdpey', 'utdpe', 'utasn', 'utosd', 'utca2', 'utsac', 'utcnap', 'utcnar', 'utcnal', 'utccy', 'utamts', 'matdt', 'issdt', 'ddate', 'xdate']
    !! WARNING [K3TBL]: expected columns not found after normalization: ['utmat', 'utmm1']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    Unique values in UTREF: ['DLG', 'ISV', 'PSD', 'DRI', 'AFS', 'AFSLIQ', '', 'INV']
    Unique values in UTSTY: ['MTB', 'MGI', 'ISD', 'MGS', 'ITB', 'PBA', 'CB1', 'LDC', '', 'DIC', 'ISB', 'DIM', 'DBD']
    Sample rows (first 3):
      Row 1:
        utref: 
        utsty: 
        utdlp: 
        utcus: 
        utctp: 
        utamoc: None
        utdpf: None
      Row 2:
        utref: AFS
        utsty: CB1
        utdlp: MSP
        utcus: OSKIBB
        utctp: BM
        utamoc: 80000000.0
        utdpf: 0.0
      Row 3:
        utref: AFS
        utsty: CB1
        utdlp: MSP
        utcus: OSKIBB
        utctp: BM
        utamoc: 100000000.0
        utdpf: 0.0
  K3TBL warning: unsupported operand type(s) for -: 'NoneType' and 'NoneType'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDLCRM.py", line 712, in process_k3tbl
    amount = utamoc - utdpf
TypeError: unsupported operand type(s) for -: 'NoneType' and 'NoneType'
  K3TBL records: 0
  Total treasury records: 0

4. Processing DCIWH.DCID...
  Using DCI file: dcid0731.sas7bdat
    Successfully read: dcid0731.sas7bdat (230 rows, 33 columns)
    Columns (33): ['ticketno', 'custname', 'newic', 'salesid', 'custcode', 'invcurrac', 'altcurrac', 'accint', 'rollover', 'convertind', 'dealerid', 'managerid', 'custicketno', 'branch', 'product', 'invcurr', 'altcurr', 'invamt', 'altamt', 'tenor', 'strikert', 'spotrt', 'dcirt', 'mmrt', 'premrec', 'prempaid', 'unwindcost', 'newdeal', 'tradedt', 'startdt', 'fixingdt', 'matdt', 'statusind']
    Sample rows (first 3):
      Row 1:
        matdt: 24321.0
        startdt: 24314.0
        invamt: 50000.0
        invcurr: MYR
        custcode: 78.0
        product: DCI
        ticketno: Z30575
      Row 2:
        matdt: 24331.0
        startdt: 24300.0
        invamt: 100000.0
        invcurr: MYR
        custcode: 78.0
        product: DCI
        ticketno: Z30173
      Row 3:
        matdt: 24335.0
        startdt: 24303.0
        invamt: 100000.0
        invcurr: MYR
        custcode: 78.0
        product: DCI
        ticketno: Z30285
  DCI warning: '>' not supported between instances of 'float' and 'datetime.date'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDLCRM.py", line 1074, in process_dci
    if matdt and startdt and matdt > rep_date['date'] and startdt <= rep_date['date']:
TypeError: '>' not supported between instances of 'float' and 'datetime.date'
  DCI records: 0

5. Processing CIS.CUSTDLY (parquet)...
  Using CIS file: CIS_CUST_DAILY.parquet
    Successfully read: CIS_CUST_DAILY.parquet (33049519 rows, 99 columns)
    !! WARNING [CIS_CUST_DAILY]: expected columns not found after normalization: ['newic']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    CIS equity rows after filter: 0
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
      Columns in utms260731.sas7bdat (99): ['branch', 'dealref', 'dealtype', 'portref', 'depotid', 'depotfd', 'custno', 'custloc', 'custname', 'custeqno', 'custeqtp', 'custacc', 'custid', 'custfiss', 'secno', 'currency', 'sectype', 'secdesc', 'secissr', 'issrloc', 'issrname', 'issracpn', 'issreqtp', 'issracc', 'issrid', 'issrtp2', 'couponrt', 'capvalue', 'dealdesc', 'faltvalu', 'facevalu', 'capprice', 'discquot', 'saledp', 'tranno', 'trantype', 'qtyldmat', 'custanal', 'custsana', 'custresc', 'custprtc', 'custrskc', 'issrprat', 'issrarat', 'dealrcd', 'brokrcd', 'brokamt', 'sundref', 'intacr', 'acprft', 'acprftys', 'amtacrin', 'camtowne', 'amtowned', 'bookvalu', 'discprm', 'mrktvalu', 'discpltd', 'discprtd', 'undiscpr', 'discprmn', 'discprpf', 'orgdisc', 'yieldmat', 'porttype', 'baserate', 'capflat', 'coupfreq', 'couprate', 'coupspre', 'issrsund', 'issrresc', 'issrprtc', 'issracpt', 'intbear', 'issprice', 'isssize', 'priceind', 'seclegcd', 'secorgcd', 'mrktmkr1', 'stdsecdc', 'race', 'busdate', 'trddate', 'valudate', 'matdate', 'issdate', 'lstcpndt', 'nxtcpndt', 'cstlc', 'cltlc', 'deal_folder', 'reval_accrued_amt', 'type_of_deal', 'fo_deal_id', 'unrealise_profit_loss', 'exp_credit_loss', 'capital_instrument_type']
      Added 20096 records from utms260731.sas7bdat

    Looking for utfx files...
      Pattern 'utfx260731.sas7bdat' matched 1 file(s):
        - utfx260731.sas7bdat
      Using: utfx260731.sas7bdat
    Successfully read: utfx260731.sas7bdat (6061 rows, 39 columns)
      Columns in utfx260731.sas7bdat (39): ['branch', 'dealtype', 'dealref', 'applcode', 'basictyp', 'custno', 'custloc', 'custname', 'custeqno', 'custeqtp', 'custacc', 'custid', 'custfiss', 'movetype', 'movesub', 'purchcur', 'salescur', 'amtpay', 'mmpriamt', 'amtrecei', 'exchrate', 'custanal', 'custsana', 'custresc', 'custprtc', 'custrskc', 'custgrp', 'dealcode', 'brokrcd', 'brokamt', 'intlsnbd', 'totint', 'optdeal', 'custpart', 'race', 'mayeqamt', 'busdate', 'strtdate', 'matdate']
      Added 6061 records from utfx260731.sas7bdat

    Looking for utrp files...
      Pattern 'utrp260731.sas7bdat' matched 1 file(s):
        - utrp260731.sas7bdat
      Using: utrp260731.sas7bdat
    Successfully read: utrp260731.sas7bdat (66 rows, 79 columns)
      Columns in utrp260731.sas7bdat (79): ['branch', 'dealtype', 'dealref', 'portref', 'depotid', 'custno', 'custloc', 'custname', 'custeqno', 'custeqtp', 'custacc', 'custid', 'custfiss', 'secno', 'currency', 'sectype', 'secdesc', 'secissr', 'issrloc', 'issrname', 'issracpn', 'issreqtp', 'issracc', 'issrid', 'issrtp2', 'pchpric', 'salepric', 'cernvalu', 'certpchp', 'cersalep', 'trantype', 'facevalu', 'tpchproc', 'tsalproc', 'rpintrat', 'custanal', 'custsana', 'custresc', 'custprtc', 'custrskc', 'brokrcd', 'brokamt', 'porttype', 'origport', 'baserate', 'capflat', 'coupfreq', 'couprate', 'coupspre', 'issranal', 'issrsund', 'issrresc', 'issrprtc', 'issracpt', 'issprice', 'seclegcd', 'secorgcd', 'stdpoors', 'mrktmkr1', 'rrepref', 'rrbrpamt', 'rrsosamt', 'curowamt', 'curboamt', 'intacrdt', 'custpart', 'custfutu', 'indprc', 'indyld', 'busdate', 'reposrtd', 'repomatd', 'dealdate', 'issdate', 'matdate', 'nxtcpndt', 'lstcpndt', 'tpchproc_fcy', 'tsalproc_fcy']
      Added 66 records from utrp260731.sas7bdat

    Total UTSAS records: 26,223
  UTSAS records: 26,200

7. Processing LCR.FD/SA/CA/FCYCA...
    Successfully read: fdhold.sas7bdat (57 rows, 8 columns)
    [fd] Columns (8): ['bnmcode', 'curcode', 'amount', 'item', 'fdpledge1', 'fxpledge1', 'fdpledge2', 'fxpledge2']
    !! WARNING [core_banking:fd]: expected columns not found after normalization: ['custcd', 'custcdx', 'acctno', 'custno', 'rem30d', 'remmth']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    Successfully read: fd30.sas7bdat (2656534 rows, 11 columns)
    [fd] Columns (11): ['branch', 'acctno', 'custcd', 'amount', 'product', 'intplan', 'curcode', 'fdhold', 'remmth', 'rem30d', 'bnmcode']
    !! WARNING [core_banking:fd]: expected columns not found after normalization: ['custcdx', 'custno']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    Successfully read: sa30.sas7bdat (4238423 rows, 7 columns)
    [sa] Columns (7): ['custcd', 'branch', 'acctno', 'product', 'amount', 'curcode', 'bnmcode']
    !! WARNING [core_banking:sa]: expected columns not found after normalization: ['custcdx', 'custno', 'rem30d', 'remmth']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    Successfully read: ca30.sas7bdat (825297 rows, 9 columns)
    [ca] Columns (9): ['custcd', 'branch', 'acctno', 'product', 'amount', 'curcode', 'intrate', 'billerind', 'bnmcode']
    !! WARNING [core_banking:ca]: expected columns not found after normalization: ['custcdx', 'custno', 'rem30d', 'remmth']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
    Successfully read: fcyca30.sas7bdat (71165 rows, 9 columns)
    [fcyca] Columns (9): ['branch', 'acctno', 'product', 'amount', 'curcode', 'intrate', 'billerind', 'custcd', 'bnmcode']
    !! WARNING [core_banking:fcyca]: expected columns not found after normalization: ['custcdx', 'custno', 'rem30d', 'remmth']. These will default to 0/''/None and likely cause dropped/zeroed records. Check real column names below.
  Banking records: 7,791,476

8. Processing CISDP/CISCA.DEPOSIT...
  CIS info records: 0

9. Processing LIST.LCR_ECP...
    Successfully read: lcr_ecp.sas7bdat (92893 rows, 8 columns)
  ECP records: 92,893

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
  Consolidated to 30 BNM code x currency combinations

Generating LCR report (text format)...
  ✓ lcr31.txt: 2 items x 5 columns

============================================================
SUMMARY
============================================================

Total: RM 285,363,833K

By Source:
  banking_fd: RM 187,846,660K
  banking_ca: RM 59,320,765K
  banking_sa: RM 35,274,335K
  banking_fcyca: RM 2,922,072K

============================================================
✓ EIBDLCRM Complete
============================================================
