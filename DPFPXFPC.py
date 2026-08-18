# PROVISIO
if provi is not None:
    # Add missing columns with defaults
    provi = provi.with_columns([
        pl.lit(0).cast(pl.Int64).alias("apcode"),
        pl.lit(0).cast(pl.Int64).alias("oldbrh"),
        pl.col("ficode").cast(pl.Utf8).fill_null("").alias("ficode_str"),
        pl.lit("     ").alias("ficody"),
        pl.lit("MYR").alias("forcurr"),
        pl.lit("N").alias("pdbind"),
        pl.col("faccode").fill_null("").alias("faccode") if 'faccode' in provi.columns else pl.lit("").alias("faccode"),
        pl.lit(0).cast(pl.Int64).alias("curbal"),
        pl.lit(0).cast(pl.Int64).alias("tenor_int"),
        pl.lit(0).cast(pl.Int64).alias("oth_charge"),
        pl.lit(0).cast(pl.Int64).alias("iisamt"),
        pl.lit(0).cast(pl.Int64).alias("totiisr"),
        pl.lit(0).cast(pl.Int64).alias("writeoff")
    ])
    
    provisio_output = provi.select([
        pl.col("ficody").alias("FICODY"),
        pl.col("ficode").cast(pl.Int64, strict=False).fill_null(0).alias("FICODE"),
        pl.col("apcode").cast(pl.Int64).alias("APCODE"),
        pl.col("acctnox").cast(pl.Int64, strict=False).fill_null(0).alias("ACCTNO"),
        pl.col("facility").fill_null("").alias("FACILITY"),
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.col("classify").fill_null("P").alias("CLASSIFY"),
        pl.col("arrears").cast(pl.Int64, strict=False).fill_null(0).alias("ARREARS"),
        pl.col("curbal").cast(pl.Int64).alias("CURBAL"),
        pl.col("tenor_int").cast(pl.Int64).alias("TENOR_INT"),
        pl.col("oth_charge").cast(pl.Int64).alias("OTH_CHARGE"),
        pl.lit(0).cast(pl.Int64).alias("REALISVL"),
        pl.lit(0).cast(pl.Int64).alias("IISOPBAL"),
        pl.col("iisamt").cast(pl.Int64).alias("TOTIIS"),
        pl.col("totiisr").cast(pl.Int64).alias("TOTIISR"),
        pl.col("writeoff").cast(pl.Int64).alias("TOTWOF"),
        pl.lit(0).cast(pl.Int64).alias("IISDANAH"),
        pl.lit(0).cast(pl.Int64).alias("IISTRANS"),
        pl.lit(0).cast(pl.Int64).alias("SPOPBAL"),
        pl.lit(0).cast(pl.Int64).alias("SPCHARGE"),
        pl.lit(0).cast(pl.Int64).alias("SPWBAMT"),
        pl.lit(0).cast(pl.Int64).alias("SPWOAMT"),
        pl.lit(0).cast(pl.Int64).alias("SPDANAH"),
        pl.lit(0).cast(pl.Int64).alias("SPTRANS"),
        pl.lit(" ").alias("GP3IND"),
        pl.col("oldbrh").cast(pl.Int64).alias("OLDBRH"),
        pl.col("faccode").fill_null("").alias("FACCODE"),
        pl.col("impaired").fill_null("N").alias("IMPAIRED"),
        pl.col("forcurr").fill_null("MYR").alias("FORCURR"),
        pl.lit(0).cast(pl.Int64).alias("TOTILM"),
        pl.col("pdbind").alias("PDBIND")
    ])
    
    provisio_spec = [
        ("FICODY", 5, 'S'), ("FICODE", 4, 'Z'), ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'), ("FACILITY", 5, 'S'), ("REPTDAY", 2, 'S'),
        ("REPTMON", 2, 'S'), ("REPTYEAR", 4, 'S'), ("CLASSIFY", 1, 'S'),
        ("ARREARS", 3, 'Z'), ("CURBAL", 17, 'Z'), ("TENOR_INT", 17, 'Z'),
        ("OTH_CHARGE", 16, 'Z'), ("REALISVL", 17, 'Z'), ("IISOPBAL", 17, 'Z'),
        ("TOTIIS", 17, 'Z'), ("TOTIISR", 17, 'Z'), ("TOTWOF", 17, 'Z'),
        ("IISDANAH", 17, 'Z'), ("IISTRANS", 17, 'Z'), ("SPOPBAL", 17, 'Z'),
        ("SPCHARGE", 17, 'Z'), ("SPWBAMT", 17, 'Z'), ("SPWOAMT", 17, 'Z'),
        ("SPDANAH", 17, 'Z'), ("SPTRANS", 17, 'Z'), ("GP3IND", 1, 'S'),
        ("OLDBRH", 5, 'Z'), ("FACCODE", 5, 'Z'), ("IMPAIRED", 1, 'S'),
        ("FORCURR", 3, 'S'), ("TOTILM", 17, 'Z'), ("PDBIND", 1, 'S')
    ]
    
    write_fixed_width(provisio_output, BASE_OUTPUT / f"PROVISIO_{output_suffix}.txt", provisio_spec)
    print(f"PROVISIO written: {provisio_output.height} records")
