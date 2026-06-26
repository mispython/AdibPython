============================================================
EIFLTEXP PROCESSING STARTED
============================================================

[STEP 1] Building DEPOSIT dataset...
  Loading MNI SAVG124...
    Reading: savg124.sas7bdat
      Available columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran']...
      Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']
      Loaded: 4,241,108 records
  Loading IMNI SAVG124...
    Reading: savg124.sas7bdat
      Available columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'bankno', 'branch', 'acctno', 'name', 'ledgbal', 'lasttran']...
      Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']
      Loaded: 2,262,899 records
  Loading MNI CURN124...
    Reading: curn124.sas7bdat
      Available columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'branch', 'acctno', 'name', 'purpose', 'sector', 'ledgbal']...
      Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']
      Loaded: 915,692 records
  Loading IMNI CURN124...
    Reading: curn124.sas7bdat
      Available columns: ['custcd', 'prodcd', 'statecd', 'amtind', 'branch', 'acctno', 'name', 'purpose', 'sector', 'ledgbal']...
      Selected columns: ['acctno', 'product', 'curbal', 'ledgbal', 'amtind', 'intpaybl', 'branch']
      Loaded: 154,763 records
  Loading MNI FDMTHLY...
    Reading: fdmthly.sas7bdat
      Available columns: ['state', 'custcode', 'bic', 'lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal']...
      Selected columns: ['acctno', 'branch', 'intplan', 'curbal', 'bic', 'amtind', 'intpay']
      Loaded: 2,756,145 records
  Loading IMNI FDMTHLY...
    Reading: fdmthly.sas7bdat
      Available columns: ['lstmatdt', 'branch', 'acctno', 'purpose', 'name', 'openind', 'curbal', 'orgdate', 'matdate', 'rate']...
      Selected columns: ['acctno', 'branch', 'intplan', 'curbal', 'bic', 'amtind', 'intpay']
      Loaded: 431,257 records
  MNI CURN filtered: 915,427 records
  IMNI CURN filtered: 154,757 records
  MNI FDMTHLY renamed
  IMNI FDMTHLY renamed
  Added dataset: 4,241,108 records
  Added dataset: 2,262,899 records
  Added dataset: 915,427 records
  Added dataset: 154,757 records
  Added dataset: 2,756,145 records
  Added dataset: 431,257 records

  Combined DEPOSIT: 10,761,593 records

[STEP 2] Applying filters...
  After PROGCD filter: 0
  After PRODUCT=166: 0
  After PROGCD special: 0
  After PRODUCT filter: 0
  After INTPAYBL: 0

  WARNING: DEPOSIT is empty after all filters!
