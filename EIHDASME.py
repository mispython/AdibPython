import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
PATHS = {
    'dp': BASE_PATH / "dp",
    'eqwh': BASE_PATH / "eqwh",
    'bnmk': BASE_PATH / "bnmk",
    'ciseq': BASE_PATH / "ciseq",
    'cisdp': BASE_PATH / "cisdp",
    'cisdp2': BASE_PATH / "cisdp2",
    'dciwh': BASE_PATH / "dciwh",
    'output': BASE_PATH / "output"
}

# ==================== DATE SETUP ====================
reptdate_val = pl.read_parquet(PATHS['dp'] / "REPTDATE.parquet")[0, "REPTDATE"]
REPTMON, REPTYEAR = f"{reptdate_val.month:02d}", str(reptdate_val.year)[-2:]
REPTMMMYY = reptdate_val.strftime("%b%y").upper()
TDATE = reptdate_val

print(f"Report Date: {reptdate_val.strftime('%d-%b-%Y').upper()}")

# ==================== MAPPINGS ====================
SME_CODES = ['41','42','43','44','46','47','48','49','51','52','53','54','66','67','68','69']
DNBFI_CODES = ['04','05','06','30','31','32','33','34','35','36','37','38','39','40','45']
FX_CODES = ['87','88','89']
VALID_CODES = SME_CODES + DNBFI_CODES + FX_CODES

CUST_GROUP_MAP = {**{c: 'SMALL MEDIUM ENTERPRISE (SMES)' for c in SME_CODES},
                  **{c: 'DOMESTIC (DNBFI)' for c in DNBFI_CODES + ['4','5','6']},
                  **{c: 'NON-RESIDENT/FOREIGN ENTITIES' for c in FX_CODES}}

CUST_TYPE_MAP = {
    **{c: 'BUMIPUTRA CONTROLLED SME' for c in ['41','42','43','66']},
    **{c: 'NON-BUMIPUTRA CONTROLLED SME' for c in ['44','46','47','67']},
    **{c: 'NON-RESIDENT CONTROLLED SME' for c in ['48','49','51','68']},
    **{c: 'GOVERNMENT CONTROLLED SME' for c in ['52','53','54','69']},
    '04': 'SUBSIDIARY STOCKBROKING COS.', '05': 'ASSOCIATE STOCKBROKING COS.',
    '06': 'OTHER STOCKBROKING COS.', '30': 'DOMESTIC OTHER NBFI',
    '31': 'SAVING INSTITUTIONS', '32': 'CREDIT CARD COMPANIES',
    '33': 'DEVELOPMENT FINANCE INSTITUTION', '34': 'BUILDING SOCIETIES',
    '35': 'CO-OPERATIVE SOCIESTIES', '36': 'STOCKBROKING COMPANIES',
    '37': 'COMMODITY BROKERS', '38': 'CREDIT AND LEASING COMPANIES',
    '39': 'UNIT TRUST COMPANIES', '40': 'INSURANCE COS AND INSURANCE RELATED ENT.',
    '45': 'REHABILITATION INSTITUTIONS',
    **{c: 'FOREIGN BUSINESS ENTERPRISES' for c in FX_CODES}
}

# ==================== HELPER FUNCTIONS ====================
def process_account_type(file, filters, custfiss_logic=None):
    """Generic account processing function"""
    df = pl.read_parquet(PATHS['dp'] / file).filter(filters)
    if custfiss_logic:
        df = df.with_columns([custfiss_logic.alias("CUSTFISS")])
    else:
        df = df.with_columns([pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2).alias("CUSTFISS")])
    
    return df.filter(
        (pl.col("CUSTFISS").is_in(VALID_CODES)) | 
        (pl.col("DNBFISME").is_in(['1','2','3']) if "DNBFISME" in df.columns else pl.lit(False))
    ).select(["ACCTNO", "CUSTFISS"] + (["DNBFISME"] if "DNBFISME" in df.columns else []) + ["CURBAL"])

def add_customer_info(df):
    """Add customer group and type mappings"""
    return df.with_columns([
        df["CUSTFISS"].map_dict(CUST_GROUP_MAP).alias("CUSTGROUP"),
        df["CUSTFISS"].map_dict(CUST_TYPE_MAP).alias("CUSTTYPE"),
        pl.col("CURBAL").alias("AMOUNT")
    ])

def write_report(filename, title, data, headers):
    """Write formatted report with ASA control"""
    lines = [
        "\x0c" + " " * 40 + "PUBLIC BANK BERHAD",
        " " * 40 + title,
        " " * 40 + f"@ {REPTMMMYY}",
        "", "=" * 100, headers
    ]
    
    for row in data.iter_rows(named=True):
        lines.append("".join(f"{str(row.get(k, '')):>{w if i > 0 else -w}}" 
                            for i, (k, w) in enumerate([('CUSTTYPE', 40), ('AMOUNT', 20)])))
    
    with open(PATHS['output'] / filename, 'w') as f:
        for i, line in enumerate(lines):
            f.write(("\x0c" if i % 60 == 0 and i > 0 else " ") + line + '\n')

# ==================== MNI DATA ====================
print("Processing MNI data...")
ca_df = process_account_type(
    "CURRENT.parquet",
    (~pl.col("OPENIND").is_in(["B","C","P"])) & 
    (((pl.col("PRODUCT") >= 400) & (pl.col("PRODUCT") <= 444)) | pl.col("PRODUCT").is_in([63,163,413])) &
    (pl.col("CURBAL") > 0) & (~pl.col("PRODUCT").is_in([63,163,413])),
    pl.when(pl.col("PRODUCT")==104).then(pl.lit("02"))
      .when(pl.col("PRODUCT")==105).then(pl.lit("81"))
      .otherwise(pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2))
).with_columns([pl.lit("CA").alias("PRODCD")])

sa_df = process_account_type("SAVING.parquet", (~pl.col("OPENIND").is_in(["B","C","P"])) & (pl.col("CURBAL") > 0)).with_columns([pl.lit("SA").alias("PRODCD")])
uma_df = process_account_type("UMA.parquet", (pl.col("OPENIND").is_in(["D","O"])) & (pl.col("CURBAL") > 0))
fd_df = process_account_type("FD.parquet", pl.col("OPENIND").is_in(["D","O"]))

mni_df = add_customer_info(pl.concat([ca_df, sa_df, uma_df, fd_df]))

# ==================== EQ DATA ====================
print("Processing EQ data...")
eq_file = PATHS['eqwh'] / f"UTMS{REPTMON}.4{REPTYEAR}.parquet"
k1_file = PATHS['bnmk'] / f"K1TBL{REPTMON}.4.parquet"

eq_list = []
if eq_file.exists():
    eq_list.append(pl.read_parquet(eq_file).drop("CUSTNAME").filter(
        pl.col("SECTYPE").is_in(['IFD','ILD','ISD','IZD','IDC','IDP','IZP']) &
        pl.col("PORTREF").is_in(['PFD','PLD','PSD','PZD','PDC']) &
        (pl.col("CUSTFISS").is_in(VALID_CODES) | ((pl.col("CUSTFISS").cast(pl.Int32) >= 30) & (pl.col("CUSTFISS").cast(pl.Int32) <= 45)))
    ).with_columns([pl.col("AMTOWNED").alias("AMOUNT"), pl.col("CUSTEQNO").alias("ACCTNO")]))

if k1_file.exists():
    eq_list.append(pl.read_parquet(k1_file).filter(
        ((pl.col("GWCCY")=="MYR") & (pl.col("GWMVT")=="P") & (pl.col("GWMVTS")=="M") & 
         (pl.col("GWDLP").is_in(['BCQ','BCD']))) |
        ((pl.col("GWDLP").str.slice(1,2).is_in(['MI','MT'])) & (pl.col("GWCCY")=="MYR") & 
         (pl.col("GWMVT")=="P") & (pl.col("GWMVTS")=="M"))
    ).with_columns([pl.col("GWC2R").alias("CUSTFISS"), pl.col("GWAN").cast(pl.Int64).alias("ACCTNO"),
                    pl.col("GWBALC").alias("AMOUNT")]).filter(
        pl.col("CUSTFISS").is_in(VALID_CODES) | ((pl.col("CUSTFISS").cast(pl.Int32) >= 30) & (pl.col("CUSTFISS").cast(pl.Int32) <= 45))
    ))

eq_combined = pl.concat(eq_list).with_columns([
    pl.col("CUSTFISS").map_dict(CUST_GROUP_MAP).alias("CUSTGROUP"),
    pl.col("CUSTFISS").map_dict(CUST_TYPE_MAP).alias("CUSTTYPE")
]) if eq_list else pl.DataFrame()

# ==================== DCI DATA ====================
print("Processing DCI data...")
dci_file = PATHS['dciwh'] / f"DCI{REPTMON}.4.parquet"
dci_df = (pl.read_parquet(dci_file).with_columns([pl.lit(TDATE).alias("REPTDATE")])
          .filter((pl.col("MATDT") > pl.col("REPTDATE")) & (pl.col("STARTDT") <= pl.col("REPTDATE")))
          .with_columns([pl.col("CUSTCODE").cast(pl.Utf8).str.zfill(2).alias("CUSTFISS"), pl.col("INVAMT").alias("AMOUNT")])
          .filter(pl.col("CUSTFISS").is_in(VALID_CODES) | ((pl.col("CUSTFISS").cast(pl.Int32) >= 30) & (pl.col("CUSTFISS").cast(pl.Int32) <= 45)))
          .with_columns([pl.col("CUSTFISS").map_dict(CUST_GROUP_MAP).alias("CUSTGROUP"),
                        pl.col("CUSTFISS").map_dict(CUST_TYPE_MAP).alias("CUSTTYPE")])
) if dci_file.exists() else pl.DataFrame()

# ==================== MERGE WITH NAMES ====================
print("Merging customer names...")
cisdp = pl.concat([pl.read_parquet(PATHS[k] / "DEPOSIT.parquet").select(["ACCTNO","CUSTNAME"]) 
                  for k in ['cisdp','cisdp2']]).unique(subset=["ACCTNO"])
ciseq = pl.read_parquet(PATHS['ciseq'] / "DEPOSIT.parquet").select(["ACCTNO","CUSTNAME"])

mni_final = mni_df.join(cisdp, on="ACCTNO", how="left") if len(mni_df) > 0 else mni_df
eq_final = eq_combined.join(ciseq, on="ACCTNO", how="left") if len(eq_combined) > 0 else eq_combined

# ==================== SUMMARIZE ====================
print("Generating summaries...")
all_data = pl.concat([df.select(["CUSTGROUP","CUSTTYPE","AMOUNT"]) 
                     for df in [mni_final, eq_final, dci_df] if len(df) > 0]).filter(pl.col("CUSTGROUP").is_not_null())

summary = all_data.group_by(["CUSTGROUP","CUSTTYPE"]).agg([pl.col("AMOUNT").sum()])
grand_total = all_data.select(pl.col("AMOUNT").sum().alias("AMOUNT")).with_columns([pl.lit("GRAND TOTAL").alias("CUSTTYPE")])

# Split by group and add totals
for group, prefix in [('SMALL MEDIUM ENTERPRISE (SMES)', 'SME'),
                      ('DOMESTIC (DNBFI)', 'DNBFI'),
                      ('NON-RESIDENT/FOREIGN ENTITIES', 'FX')]:
    df = summary.filter(pl.col("CUSTGROUP") == group)
    if len(df) > 0:
        total_row = df.select(pl.col("AMOUNT").sum().alias("AMOUNT")).with_columns([pl.lit("TOTAL").alias("CUSTTYPE")])
        df = pl.concat([df, total_row])
        write_report(f"{prefix}_REPORT_{REPTMMMYY}.txt", 
                    f"DEPOSITS FROM {group}", df.sort("CUSTTYPE"),
                    f"{'CUSTOMER':<40} {'TOTAL OS MYR':>20}")
        print(f"{prefix} Total: {total_row['AMOUNT'][0]:,.2f}")

# Grand total
with open(PATHS['output'] / f"GRAND_TOTAL_{REPTMMMYY}.txt", 'w') as f:
    f.write(f"\x0c{'GRAND TOTAL':<40} {grand_total[0,'AMOUNT']:>20,.2f}\n")

print(f"Grand Total: {grand_total[0,'AMOUNT']:,.2f}")

# ==================== DETAILED LISTS ====================
print("Generating detailed lists...")
for df, name, cols in [(eq_final, 'EQ', ['ACCTNO','CUSTNAME','CUSTTYPE','AMOUNT']),
                       (dci_df, 'DCI', ['TICKETNO','CUSTNAME','CUSTTYPE','AMOUNT']),
                       (mni_final, 'MNI', ['ACCTNO','CUSTNAME','CUSTFISS','DNBFISME','PRODCD','CUSTTYPE','AMOUNT'])]:
    if len(df) > 0:
        with open(PATHS['output'] / f"{name}_LIST_{REPTMMMYY}.txt", 'w') as f:
            f.write("\x0c" + " "*40 + "PUBLIC BANK BERHAD\n")
            f.write(" "*40 + f"{name} CUSTOMER LIST\n")
            f.write(" "*40 + f"@ {REPTMMMYY}\n\n" + "="*100 + "\n")
            f.write(" ".join(f"{c:<{15 if c=='AMOUNT' else 12 if c=='ACCTNO' else 40 if c=='CUSTNAME' else 30 if c=='CUSTTYPE' else 10}}" for c in cols) + "\n")
            for row in df.sort(cols[0]).iter_rows(named=True):
                f.write(" ".join(f"{str(row.get(c,'')):<{15 if c=='AMOUNT' else 12 if c=='ACCTNO' else 40 if c=='CUSTNAME' else 30 if c=='CUSTTYPE' else 10}}" for c in cols) + "\n")

print("Processing complete!")
