    log("  reading LNCOMM ...")
    lncomm = read_sas7bdat(LOAN_LNCOMM_SAS.format(reptmon=rv['reptmon']),
                           usecols=LNCOMM_KEEP)
    log(f"  LNCOMM raw: {len(lncomm):>10,} rows")

    # WHERE ENTITY_CD = 'PIBB'
    if not lncomm.empty:
        if 'entity_cd' in lncomm.columns:
            lncomm = lncomm[
                lncomm['entity_cd'].fillna('').astype(str).str.strip().str.upper() == 'PIBB'
            ].drop(columns='entity_cd')
            log(f"  LNCOMM after ENTITY_CD='PIBB': {len(lncomm):>10,} rows")
        else:
            log("  WARNING: ENTITY_CD not found in LNCOMM; filter not applied")
