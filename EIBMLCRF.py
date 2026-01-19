import polars as pl
from pathlib import Path

def eibmlcrf():
    base = Path.cwd()
    reptdate = pl.read_parquet(base / "LN/REPTDATE.parquet")["REPTDATE"][0]
    yr, mon = str(reptdate.year)[-2:], f"{reptdate.month:02d}"
    
    # CFINASST
    cf1 = pl.read_parquet(base / f"COLL/CFINASST{mon}{yr}.parquet", columns=["ACCTNO","NOTENO","FDACCTNO","FDCDNO"])
    cf2 = pl.read_parquet(base / f"ICOLL/ICFINASST{mon}{yr}.parquet", columns=["ACCTNO","NOTENO","FDACCTNO","FDCDNO"])
    cfinasst = pl.concat([cf1, cf2]).sort(["ACCTNO", "NOTENO"])
    
    # LOAN
    ln1 = pl.read_parquet(base / f"LN/LN{mon}.4{yr}.parquet", columns=["ACCTNO","NOTENO","BALANCE","EXPRDATE"])
    ln2 = pl.read_parquet(base / f"ILN/ILN{mon}.4{yr}.parquet", columns=["ACCTNO","NOTENO","BALANCE","EXPRDATE"])
    loan = pl.concat([ln1, ln2]).sort(["ACCTNO", "NOTENO"])
    
    # MERGE: CFINASST(IN=A) LOAN; BY ACCTNO NOTENO; IF A;
    merged = cfinasst.join(loan, on=["ACCTNO", "NOTENO"], how="inner")
    merged = merged.rename({"ACCTNO": "LNACCTNO", "FDACCTNO": "ACCTNO", "FDCDNO": "CDNO"})
    merged = merged.sort(["ACCTNO", "CDNO"])
    
    # FD
    fd1 = pl.read_parquet(base / "LCR/FD.parquet", columns=["ACCTNO","CDNO","FDHOLD"])
    fd2 = pl.read_parquet(base / "ILCR/FD.parquet", columns=["ACCTNO","CDNO","FDHOLD"])
    fd = pl.concat([fd1, fd2]).rename({"FDHOLD": "FDHOLD_ORI"}).sort(["ACCTNO", "CDNO"])
    
    # LCR.TEMPFD: MERGE FD(IN=A) CFINASST(IN=B); BY ACCTNO CDNO; IF A;
    temp = fd.join(merged, on=["ACCTNO", "CDNO"], how="left")
    
    # Apply conditions only when both A and B have data (LNACCTNO not null)
    has_b = temp["LNACCTNO"].is_not_null()
    cond = (
        ((temp["LNACCTNO"] >= 2500000000) & (temp["LNACCTNO"] <= 2599999999)) |
        ((temp["LNACCTNO"] >= 2850000000) & (temp["LNACCTNO"] <= 2859999999)) |
        ((temp["LNACCTNO"] >= 3000000000) & (temp["LNACCTNO"] <= 3999999999)) |
        (((temp["EXPRDATE"] - reptdate).dt.total_days() > 30) & (temp["BALANCE"] > 0))
    )
    
    temp = temp.with_columns(
        pl.when(has_b & cond).then("Y").otherwise(None).alias("FDPLED"),
        pl.when(pl.col("FDPLED") == "Y").then("Y").otherwise(pl.col("FDHOLD_ORI")).alias("FDHOLD")
    ).filter(pl.col("FDHOLD") == "Y").select(["ACCTNO", "CDNO", "FDHOLD", "FDPLED"]).unique(["ACCTNO", "CDNO"])
    
    # Update LCR.FD and ILCR.FD
    for path in [base / "LCR/FD.parquet", base / "ILCR/FD.parquet"]:
        if path.exists():
            df = pl.read_parquet(path).rename({"FDHOLD": "FDHOLD_ORI"})
            df = df.join(temp, on=["ACCTNO", "CDNO"], how="left", suffix="_temp")
            df = df.with_columns(
                pl.coalesce(pl.col("FDHOLD_temp"), pl.col("FDHOLD_ORI")).alias("FDHOLD"),
                pl.coalesce(pl.col("FDPLED"), pl.lit(None)).alias("FDPLED")
            ).drop(["FDHOLD_ORI", "FDHOLD_temp"])
            df.write_parquet(path)
    
    temp.write_parquet(base / "output/TEMPFD.parquet")
    return temp

if __name__ == "__main__":
    eibmlcrf()
