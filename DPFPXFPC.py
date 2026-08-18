# =========================================================
# 14. WRITE OUTPUT FILES
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# ACCTCRED
if acct is not None:
    acctcred_output = acct.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64).alias("ACCTNO"),
        pl.lit("MYR").alias("CURRENCY"),
        pl.lit(0).cast(pl.Int64).alias("APPRLIMT"),
        pl.lit(0).cast(pl.Int64).alias("APPRLIM2"),
        pl.col("issuedd").cast(pl.Int64).alias("ISSUEDD"),
        pl.col("issuemm").cast(pl.Int64).alias("ISSUEMM"),
        pl.col("issueya").cast(pl.Int64).alias("ISSUEYA"),
        pl.col("issueyy").cast(pl.Int64).alias("ISSUEYY"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("lmtamt").cast(pl.Int64).alias("LMTAMT"),
        pl.lit(0).cast(pl.Int64).alias("AALIMIT"),
        pl.col("allrefno").fill_null("").alias("ALLREFNO"),
        pl.lit(0).cast(pl.Int64).alias("LEGAL_ACTION_CD"),
        pl.col("ladtdd").cast(pl.Int64).alias("LADTDD"),
        pl.col("ladtmm").cast(pl.Int64).alias("LADTMM"),
        pl.col("ladtyy").cast(pl.Int64).alias("LADTYY"),
        pl.col("fxrate").cast(pl.Int64).alias("FXRATE"),
        pl.col("climate_prin_taxonomy_class").alias("CLIMATE_PRIN_TAXONOMY_CLASS")
    ])
    
    acctcred_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("CURRENCY", 3, 'S'), ("APPRLIMT", 24, 'Z'),
        ("APPRLIM2", 16, 'Z'), ("ISSUEDD", 2, 'Z'), ("ISSUEMM", 2, 'Z'),
        ("ISSUEYA", 2, 'Z'), ("ISSUEYY", 2, 'Z'), ("OLDBRH", 5, 'Z'),
        ("LMTAMT", 16, 'Z'), ("AALIMIT", 24, 'Z'), ("ALLREFNO", 200, 'S'),
        ("LEGAL_ACTION_CD", 2, 'Z'), ("LADTDD", 2, 'Z'), ("LADTMM", 2, 'Z'),
        ("LADTYY", 4, 'Z'), ("FXRATE", 8, 'Z'), ("CLIMATE_PRIN_TAXONOMY_CLASS", 5, 'S')
    ]
    
    write_fixed_width(acctcred_output, BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt", acctcred_spec)
    print(f"ACCTCRED written: {acctcred_output.height} records")

# SUBACRED
if suba_final is not None:
    # Add missing columns with defaults
    suba_final = suba_final.with_columns([
        pl.lit(0).cast(pl.Int64).alias("fconcept"),
        pl.lit("99").alias("typeprc"),
        pl.lit(0).cast(pl.Int64).alias("intratex"),
        pl.lit(0).cast(pl.Int64).alias("commratex"),
        pl.lit(0).cast(pl.Int64).alias("discratex"),
        pl.lit(0).cast(pl.Int64).alias("combratex"),
        pl.lit("").alias("industrial_sector_cd")
    ])
    
    subacred_output = suba_final.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64).alias("ACCTNO"),
        pl.col("noteno").alias("NOTENO"),
        pl.col("facility").fill_null("").alias("FACILITY"),
        pl.col("facility").fill_null("").alias("FACILITY2"),
        pl.col("syndicat").alias("SYNDICAT"),
        pl.col("specialf").alias("SPECIALF"),
        pl.col("purposes").alias("PURPOSES"),
        pl.col("fconcept").cast(pl.Int64).alias("FCONCEPT"),
        pl.col("noteterm").cast(pl.Int64).alias("NOTETERM"),
        pl.col("payfreqc").alias("PAYFREQC"),
        pl.col("dataxx").alias("DATAXX"),
        pl.col("custcode_clean").cast(pl.Int64, strict=False).fill_null(0).alias("CUSTCODE"),
        pl.col("sector_clean").fill_null("").alias("SECTOR"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("unearned").cast(pl.Int64).alias("UNEARNED"),
        pl.col("sm_status1").alias("SM_STATUS1"),
        pl.col("sm_dat1").alias("SM_DAT1"),
        pl.col("rmsbba").alias("RMSBBA"),
        pl.col("intratex").cast(pl.Int64).alias("INTRATEX"),
        pl.col("typeprc").alias("TYPEPRC"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("sectfiss").alias("SECTFISS"),
        pl.col("custfiss").alias("CUSTFISS"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.col("tfr02i").cast(pl.Int64).alias("TFR02I"),
        pl.col("commratex").cast(pl.Int64).alias("COMMRATEX"),
        pl.col("discratex").cast(pl.Int64).alias("DISCRATEX"),
        pl.col("combratex").cast(pl.Int64).alias("COMBRATEX"),
        pl.col("sm_status").alias("SM_STATUS"),
        pl.col("sm_datestr").alias("SM_DATESTR"),
        pl.col("ia_lru").alias("IA_LRU"),
        pl.col("pdbind").alias("PDBIND"),
        pl.col("fdisbdt").alias("FDISBDT"),
        pl.col("score1").alias("SCORE1"),
        pl.col("score2").alias("SCORE2"),
        pl.col("dnbfisme").alias("DNBFISME"),
        pl.col("industrial_sector_cd").alias("INDUSTRIAL_SECTOR_CD"),
        pl.col("lu_add1").alias("LU_ADD1"),
        pl.col("lu_add2").alias("LU_ADD2"),
        pl.col("lu_add3").alias("LU_ADD3"),
        pl.col("lu_add4").alias("LU_ADD4"),
        pl.col("lu_town_city").alias("LU_TOWN_CITY"),
        pl.col("lu_postcode").alias("LU_POSTCODE"),
        pl.col("lu_state_cd").alias("LU_STATE_CD"),
        pl.col("lu_country_cd").alias("LU_COUNTRY_CD")
    ])
    
    subacred_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("NOTENO", 5, 'S'), ("FACILITY", 5, 'S'),
        ("FACILITY2", 5, 'S'), ("SYNDICAT", 1, 'S'), ("SPECIALF", 2, 'S'),
        ("PURPOSES", 4, 'S'), ("FCONCEPT", 2, 'Z'), ("NOTETERM", 3, 'Z'),
        ("PAYFREQC", 2, 'S'), ("DATAXX", 18, 'S'), ("CUSTCODE", 2, 'Z'),
        ("SECTOR", 4, 'S'), ("OLDBRH", 5, 'Z'), ("UNEARNED", 17, 'Z'),
        ("SM_STATUS1", 1, 'S'), ("SM_DAT1", 8, 'S'), ("RMSBBA", 15, 'S'),
        ("INTRATEX", 5, 'Z'), ("TYPEPRC", 2, 'S'), ("FACCODE", 5, 'Z'),
        ("SECTFISS", 4, 'S'), ("CUSTFISS", 2, 'S'), ("FORCURR", 3, 'S'),
        ("TFR02I", 1, 'Z'), ("COMMRATEX", 5, 'Z'), ("DISCRATEX", 5, 'Z'),
        ("COMBRATEX", 5, 'Z'), ("SM_STATUS", 1, 'S'), ("SM_DATESTR", 8, 'S'),
        ("IA_LRU", 1, 'S'), ("PDBIND", 1, 'S'), ("FDISBDT", 8, 'S'),
        ("SCORE1", 5, 'S'), ("SCORE2", 5, 'S'), ("DNBFISME", 1, 'S'),
        ("INDUSTRIAL_SECTOR_CD", 5, 'S'), ("LU_ADD1", 40, 'S'),
        ("LU_ADD2", 40, 'S'), ("LU_ADD3", 40, 'S'), ("LU_ADD4", 40, 'S'),
        ("LU_TOWN_CITY", 20, 'S'), ("LU_POSTCODE", 5, 'S'),
        ("LU_STATE_CD", 2, 'S'), ("LU_COUNTRY_CD", 2, 'S')
    ]
    
    write_fixed_width(subacred_output, BASE_OUTPUT / f"SUBACRED_{output_suffix}.txt", subacred_spec)
    print(f"SUBACRED written: {subacred_output.height} records")

# CREDITPO
if suba_final is not None:
    creditpo_output = suba_final.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64).alias("ACCTNO"),
        pl.col("noteno").alias("NOTENO"),
        pl.col("facility").fill_null("").alias("FACILITY"),
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.col("outstand").cast(pl.Int64).alias("OUTSTAND"),
        pl.col("arrears").cast(pl.Int64).alias("ARREARS"),
        pl.col("instalm").cast(pl.Int64).alias("INSTALM"),
        pl.col("undrawn").cast(pl.Int64).alias("UNDRAWN"),
        pl.lit("O").alias("ACCTSTAT"),
        pl.col("nodays").cast(pl.Int64).alias("NODAYS"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("biltot").cast(pl.Int64).alias("BILTOT"),
        pl.col("odxsamt").cast(pl.Int64).alias("ODXSAMT"),
        pl.col("curbal").cast(pl.Int64).alias("CURBAL"),
        pl.col("intamt").cast(pl.Int64).alias("INTAMT"),
        pl.col("oth_charge").cast(pl.Int64).alias("OTH_CHARGE"),
        pl.col("repaid").cast(pl.Int64).alias("REPAID"),
        pl.col("disburse").cast(pl.Int64).alias("DISBURSE"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.col("pdbind").alias("PDBIND"),
        pl.col("mtd_tawidh_amt").cast(pl.Int64).alias("MTD_TAWIDH_AMT"),
        pl.col("mtd_gharamah_amt").cast(pl.Int64).alias("MTD_GHARAMAH_AMT"),
        pl.lit("").alias("REPAY_SOURCE"),
        pl.lit("").alias("REPAY_TYPE_CD")
    ])
    
    creditpo_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("NOTENO", 5, 'S'), ("FACILITY", 5, 'S'),
        ("REPTDAY", 2, 'S'), ("REPTMON", 2, 'S'), ("REPTYEAR", 4, 'S'),
        ("OUTSTAND", 16, 'Z'), ("ARREARS", 3, 'Z'), ("INSTALM", 3, 'Z'),
        ("UNDRAWN", 17, 'Z'), ("ACCTSTAT", 1, 'S'), ("NODAYS", 5, 'Z'),
        ("OLDBRH", 5, 'Z'), ("BILTOT", 17, 'Z'), ("ODXSAMT", 17, 'Z'),
        ("CURBAL", 17, 'Z'), ("INTAMT", 17, 'Z'), ("OTH_CHARGE", 17, 'Z'),
        ("REPAID", 15, 'Z'), ("DISBURSE", 15, 'Z'), ("FACCODE", 5, 'Z'),
        ("FORCURR", 3, 'S'), ("PDBIND", 1, 'S'),
        ("MTD_TAWIDH_AMT", 15, 'D'), ("MTD_GHARAMAH_AMT", 15, 'D'),
        ("REPAY_SOURCE", 4, 'S'), ("REPAY_TYPE_CD", 2, 'S')
    ]
    
    write_fixed_width(creditpo_output, BASE_OUTPUT / f"CREDITPO_{output_suffix}.txt", creditpo_spec)
    print(f"CREDITPO written: {creditpo_output.height} records")

# =========================================================
# 15. PRINT SUMMARY
# =========================================================
print("\n" + "="*50)
print("PROCESSING COMPLETE")
print("="*50)
print(f"Processing Date: {TDATE.strftime('%Y-%m-%d')}")
print(f"MAST rows: {mast.height if mast is not None else 0}")
print(f"CRED rows: {cred.height if cred is not None else 0}")
print(f"SUBA rows: {suba.height if suba is not None else 0}")
print(f"ACCT rows: {acct.height if acct is not None else 0}")
print(f"BTR2 rows: {btr2.height if btr2 is not None else 0}")
print(f"SUBCR rows: {subcr.height if subcr is not None else 0}")
print(f"Final SUBA rows: {suba_final.height if suba_final is not None else 0}")
print(f"\nOutput files written to: {BASE_OUTPUT}")
print("="*50)
