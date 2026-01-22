import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

def eiqhpcr1():
    base = Path.cwd()
    
    # Run external programs
    try:
        from pbbelf import process as pbbelf_process
        pbbelf_process()
    except:
        pass
    
    try:
        from pbblnfmt import process as pbblnfmt_process
        pbblnfmt_process()
    except:
        pass
    
    # REPTDATE processing
    reptdate = pl.read_parquet(base / "LOAN/REPTDATE.parquet")["REPTDATE"][0]
    reptday = reptdate.day
    
    # Week determination
    if reptday == 8:
        wk, wk1, sdd = '1', '4', 1
    elif reptday == 15:
        wk, wk1, sdd = '2', '1', 9
    elif reptday == 22:
        wk, wk1, sdd = '3', '2', 16
    else:
        wk, wk1, sdd = '4', '3', 23
    
    mm = reptdate.month
    mm1 = mm - 1 if wk == '1' and mm > 1 else 12 if wk == '1' else mm
    
    # Date variables
    nowk, nowk1 = wk, wk1
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(reptdate.year)
    reptday_str = f"{reptdate.day:02d}"
    rdate = reptdate.strftime("%d%m%y")
    sdate_dt = datetime(reptdate.year, mm, sdd)
    sdate = sdate_dt.strftime("%d%m%y")
    
    print(f"EIQHPCR1 - HP NPL Analysis | Date: {rdate}, Week: {wk}")
    
    # Define HP products (from macro &HP)
    hp_products = [128, 380, 700]  # Example HP loan types
    
    # Read and sort datasets
    sp2_df = pl.read_parquet(base / "CCDNPL/SP2.parquet") \
              .select(["ACCTNO", "NOTENO", "SPPL", "RECOVER"]) \
              .sort(["ACCTNO", "NOTENO"])
    
    lnnote_df = pl.read_parquet(base / "LOAN/LNNOTE.parquet") \
                 .filter(
                     pl.col("LOANTYPE").is_in(hp_products) &
                     (pl.col("BALANCE") > 0)
                 ) \
                 .select([
                     "ACCTNO", "NOTENO", "LOANTYPE", "NETPROC", "APPVALUE",
                     "NOTETERM", "NAME", "BORSTAT", "BALANCE", "SCORE2",
                     "CENSUS", "BLDATE", "NTBRCH"
                 ]) \
                 .sort(["ACCTNO", "NOTENO"])
    
    # Merge datasets
    hploan_df = lnnote_df.join(sp2_df, on=["ACCTNO", "NOTENO"], how="left")
    
    if hploan_df.is_empty():
        print("No HP loan data")
        return
    
    # Process calculations
    # 1. Credit risk
    hploan_df = hploan_df.with_columns(
        pl.col("SCORE2").str.slice(0, 1).alias("CRRISK")
    )
    
    # 2. Margin of finance
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("APPVALUE") > 0)
        .then((pl.col("NETPROC") / pl.col("APPVALUE") * 100).round(1))
        .otherwise(0)
        .alias("MARGINF")
    )
    
    # 3. Margin groups
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("MARGINF") < 70).then("D. <70%")
        .when((pl.col("MARGINF") >= 70) & (pl.col("MARGINF") < 80)).then("C. 70 TO <80%")
        .when((pl.col("MARGINF") >= 80) & (pl.col("MARGINF") < 85)).then("B. 80 TO <85%")
        .otherwise("A. 85% & ABV")
        .alias("MGINGRP")
    )
    
    # 4. Loan term groups
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("NOTETERM") <= 36).then("A. <=3 YRS")
        .when((pl.col("NOTETERM") > 36) & (pl.col("NOTETERM") <= 60)).then("B. 4-5 YRS")
        .when((pl.col("NOTETERM") > 60) & (pl.col("NOTETERM") <= 84)).then("C. 6-7 YRS")
        .otherwise("D. 8-9 YRS")
        .alias("TERMGRP")
    )
    
    # 5. Vehicle make from CENSUS
    hploan_df = hploan_df.with_columns(
        pl.col("CENSUS").cast(pl.Utf8).str.zfill(7).alias("CENSUS9")
    )
    
    def get_make(census9):
        if not census9:
            return "OTHERS"
        prefix = census9[:2].strip()
        if prefix == "1": return "PROTON"
        elif prefix == "2": return "PERODUA"
        elif prefix == "3": return "TOYOTA"
        elif prefix == "4": return "NISSAN"
        elif prefix == "5": return "HONDA"
        elif prefix == "10": return "MERCEDES BENZ"
        elif prefix == "13": return "BMW"
        else: return "OTHERS"
    
    hploan_df = hploan_df.with_columns(
        pl.col("CENSUS9").map_elements(get_make, return_dtype=pl.Utf8).alias("MAKE")
    )
    
    # 6. Car type (National/Non-national)
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("MAKE").is_in(["PROTON", "PERODUA"]))
        .then("NATIONAL")
        .otherwise("NON NATIONAL")
        .alias("CARS")
    )
    
    # 7. Goods classification
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("MAKE") == "OTHERS")
        .then(
            pl.when(pl.col("LOANTYPE").is_in([128, 380, 700]))
            .then("SCHEDULE")
            .otherwise("UNSCHEDULE")
        )
        .otherwise("")
        .alias("GOODS")
    )
    
    # 8. New/Secondhand
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("CENSUS9").str.slice(3, 1).is_in(["1", "2"]))
        .then("NEW")
        .otherwise("SECONDHAND")
        .alias("NEWSEC")
    )
    
    # 9. Category
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("CENSUS9").str.slice(2, 1).is_in(["1", "2", "3"]))
        .then("P.SALOON, 4X4, M.CYCLE")
        .otherwise("VAN,LORRY,TRACTOR & OTH")
        .alias("CATGY")
    )
    
    # 10. Months in arrears (MTHARR) - same calculation as before
    def calc_mtharr(bldate):
        if not bldate or bldate <= 0:
            return 0
        try:
            thisdate = datetime.strptime(rdate, "%d%m%y")
            if isinstance(bldate, (int, float)):
                sas_base = datetime(1960, 1, 1)
                bldate_dt = sas_base + timedelta(days=int(bldate))
            else:
                bldate_dt = bldate
            daydiff = (thisdate - bldate_dt).days
            
            # MTHARR chain
            if daydiff > 729: return int((daydiff/365)*12)
            elif daydiff > 698: return 23
            elif daydiff > 668: return 22
            elif daydiff > 638: return 21
            elif daydiff > 608: return 20
            elif daydiff > 577: return 19
            elif daydiff > 547: return 18
            elif daydiff > 516: return 17
            elif daydiff > 486: return 16
            elif daydiff > 456: return 15
            elif daydiff > 424: return 14
            elif daydiff > 394: return 13
            elif daydiff > 364: return 12
            elif daydiff > 333: return 11
            elif daydiff > 303: return 10
            elif daydiff > 273: return 9
            elif daydiff > 243: return 8
            elif daydiff > 212: return 7
            elif daydiff > 182: return 6
            elif daydiff > 151: return 5
            elif daydiff > 121: return 4
            elif daydiff > 91: return 3
            elif daydiff > 59: return 2
            elif daydiff > 29: return 1
            else: return 0
        except:
            return 0
    
    hploan_df = hploan_df.with_columns(
        pl.col("BLDATE").map_elements(calc_mtharr, return_dtype=pl.Int64).alias("MTHARR")
    )
    
    # Set MTHARR=999 for BORSTAT='F' (Foreclosed)
    hploan_df = hploan_df.with_columns(
        pl.when(pl.col("BORSTAT") == "F")
        .then(999)
        .otherwise(pl.col("MTHARR"))
        .alias("MTHARR")
    )
    
    # 11. Calculate NETSP
    hploan_df = hploan_df.with_columns(
        (pl.col("SPPL").fill_null(0) - pl.col("RECOVER").fill_null(0)).alias("NETSP")
    )
    
    # Save to HPNPL directory
    hpnpl_path = base / "HPNPL"
    hpnpl_path.mkdir(exist_ok=True)
    
    # Save REPTDATE
    pl.DataFrame({"REPTDATE": [reptdate]}).write_parquet(hpnpl_path / "REPTDATE.parquet")
    
    # Save HPLOAN
    hploan_df.write_parquet(hpnpl_path / "HPLOAN.parquet")
    
    # Print summary
    print(f"Processed {len(hploan_df)} HP loans")
    print(f"Saved to: {hpnpl_path}/HPLOAN.parquet")
    
    # Show quick stats
    if len(hploan_df) > 0:
        print("\nQuick Stats:")
        print(f"  Total Balance: {hploan_df['BALANCE'].sum():,.2f}")
        print(f"  Average Margin: {hploan_df['MARGINF'].mean():.1f}%")
        print(f"  Top Make: {hploan_df['MAKE'].mode()[0] if len(hploan_df['MAKE'].mode()) > 0 else 'N/A'}")
        print(f"  Average MTHARR: {hploan_df['MTHARR'].mean():.1f} months")

def eiqhpcr1_simple():
    """Even simpler version"""
    base = Path.cwd()
    
    # Get date
    reptdate = pl.read_parquet(base / "LOAN/REPTDATE.parquet")["REPTDATE"][0]
    reptday = reptdate.day
    wk = '1' if reptday == 8 else '2' if reptday == 15 else '3' if reptday == 22 else '4'
    reptmon = f"{reptdate.month:02d}"
    rdate = reptdate.strftime("%d%m%y")
    
    print(f"EIQHPCR1 - HP Loan Analysis | {rdate} Week:{wk}")
    
    # Read data
    try:
        df = pl.read_parquet(base / "LOAN/LNNOTE.parquet")
        df = df.filter((pl.col("BALANCE") > 0))
    except:
        print("No loan data")
        return
    
    # Basic calculations
    df = df.with_columns(
        # Credit risk
        pl.col("SCORE2").str.slice(0, 1).alias("CRRISK"),
        
        # Margin
        pl.when(pl.col("APPVALUE") > 0)
        .then((pl.col("NETPROC") / pl.col("APPVALUE") * 100).round(1))
        .otherwise(0)
        .alias("MARGINF"),
        
        # Term group
        pl.when(pl.col("NOTETERM") <= 36).then("<=3 YRS")
        .when(pl.col("NOTETERM") <= 60).then("4-5 YRS")
        .when(pl.col("NOTETERM") <= 84).then("6-7 YRS")
        .otherwise("8-9 YRS")
        .alias("TERMGRP")
    )
    
    # Save
    output_path = base / "HPNPL"
    output_path.mkdir(exist_ok=True)
    df.write_parquet(output_path / "HPLOAN_SIMPLE.parquet")
    
    print(f"Saved {len(df)} loans to {output_path}/HPLOAN_SIMPLE.parquet")

if __name__ == "__main__":
    eiqhpcr1()
