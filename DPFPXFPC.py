import polars as pl
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta

def eimhptop():
    """Conventional bank version of EIMHPTOP - using SAS datasets directly"""
    base = Path.cwd()
    loan_path = base / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/"
    cis_path = base / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/"

    # Define output path
    output_path = base / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMHPTOP/HPCOLD.txt"
    print(f"Output will be saved to: {output_path}")

    # Hardcode REPTDATE (current date - 1 day)
    reptdate = datetime.now().date() - timedelta(days=13)
    reptday = reptdate.day

    # SAS SELECT logic for weeks
    if reptday == 8:
        wk, wk1 = '1', '4'
    elif reptday == 15:
        wk, wk1 = '2', '1'
    elif reptday == 22:
        wk, wk1 = '3', '2'
    else:
        wk, wk1 = '4', '3'

    mm = reptdate.month
    if wk == '1':
        mm1 = mm - 1 if mm > 1 else 12
    else:
        mm1 = mm

    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    rdate = reptdate.strftime("%d/%m/%y")

    print(f"EIMHPTOP - Conventional Bank Top Accounts")
    print(f"NOWK: {wk}, NOWK1: {wk1}, REPTMON: {reptmon}, RDATE: {rdate}")

    # HPD products
    hpd_products = [5.0, 15.0, 61.0, 70.0, 71.0, 200.0, 205.0, 210.0, 212.0, 216.0]

    # 1. CIS INFO - Read SAS dataset (no row limit for production)
    try:
        cis_df, meta = pyreadstat.read_sas7bdat(
            str(cis_path / "loan.sas7bdat")
        )
        cis_df = pl.from_pandas(cis_df)

        hpcis_df = cis_df.filter(
            (~pl.col("CACCCODE").is_in(["017", "021", "028"])) &
            (pl.col("SECCUST") == "901")
        )
        hpcis_df = hpcis_df.with_columns(
            pl.when(pl.col("NEWIC") != "")
            .then(pl.col("NEWIC"))
            .otherwise(pl.col("OLDIC"))
            .alias("ICNO")
        ).select(["ACCTNO", "ICNO", "CUSTNAME"])

        print(f"CIS sample size: {len(hpcis_df)} records")
    except Exception as e:
        print(f"Error reading CIS: {e}")
        hpcis_df = pl.DataFrame({"ACCTNO": [], "ICNO": [], "CUSTNAME": []})

    # 2. EXTRACT HP A/C with MTHARR calculation (no row limit for production)
    try:
        hpacc_df, meta = pyreadstat.read_sas7bdat(
            str(loan_path / f"loan{reptmon}{wk}.sas7bdat")
        )
        hpacc_df = pl.from_pandas(hpacc_df)
        print(f"HPACC sample size: {len(hpacc_df)} records")

        if 'MTHARR' in hpacc_df.columns:
            print("MTHARR column already exists in the data")
            hpacc_df = hpacc_df.select(["ACCTNO", "NOTENO", "PRODUCT", "BORSTAT", "BALANCE",
                                      "BLDATE", "ISSUEDT", "MTHARR", "BRANCH"])
        else:
            thisdate = datetime.strptime(reptdate.strftime("%d%m%y"), "%d%m%y")

            def calculate_mtharr(bldate):
                if not bldate or bldate == 0:
                    return 0

                try:
                    if isinstance(bldate, (int, float)):
                        sas_base = datetime(1960, 1, 1)
                        bldate_dt = sas_base + timedelta(days=int(bldate))
                    else:
                        bldate_dt = bldate

                    daydiff = (thisdate - bldate_dt).days

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
                    elif daydiff > 213: return 7
                    elif daydiff > 182: return 6
                    elif daydiff > 151: return 5
                    elif daydiff > 121: return 4
                    elif daydiff > 91: return 3
                    elif daydiff > 61: return 2
                    elif daydiff > 30: return 1
                    else: return 0
                except:
                    return 0

            hpacc_df = hpacc_df.with_columns(
                pl.col("BLDATE").map_elements(calculate_mtharr, return_dtype=pl.Int64).alias("MTHARR")
            ).select(["ACCTNO", "NOTENO", "PRODUCT", "BORSTAT", "BALANCE",
                      "BLDATE", "ISSUEDT", "MTHARR", "BRANCH"])

        hpacc_df = hpacc_df.with_columns(
            pl.col("MTHARR").fill_null(0)
        )

        print(f"PRODUCT column type: {hpacc_df['PRODUCT'].dtype}")
        print(f"PRODUCT unique values: {hpacc_df['PRODUCT'].unique().to_list()[:10]}")
        print(f"BRANCH unique values: {hpacc_df['BRANCH'].unique().to_list()[:10]}")
    except Exception as e:
        print(f"File not found or error reading: LOAN/LOAN{reptmon}{wk}.sas7bdat - {e}")
        return

    # Filter for HPD products and balance > 0
    hpacc_df = hpacc_df.filter(
        (pl.col("PRODUCT").is_in(hpd_products)) &
        (pl.col("BALANCE") > 0)
    )

    print(f"After filtering: {len(hpacc_df)} records")

    if hpacc_df.is_empty():
        print("No records match the product filters. Exiting.")
        return

    # 3. Merge with BRANCH abbreviation
    brhdata_df = None
    try:
        lkp_branch_paths = [
            base / "LKP_BRANCH",
            base / "LKP_BRANCH.txt",
            base / "LKP_BRANCH.dat",
            loan_path / "LKP_BRANCH",
            loan_path / "LKP_BRANCH.txt",
            cis_path / "LKP_BRANCH",
            cis_path / "LKP_BRANCH.txt",
            base / "../LKP_BRANCH",
            base / "../../LKP_BRANCH"
        ]

        lkp_found = False
        for lkp_branch_path in lkp_branch_paths:
            if lkp_branch_path.exists():
                print(f"Found LKP_BRANCH at: {lkp_branch_path}")

                if lkp_branch_path.suffix == '.sas7bdat' or lkp_branch_path.with_suffix('.sas7bdat').exists():
                    try:
                        if lkp_branch_path.suffix != '.sas7bdat':
                            lkp_branch_path = lkp_branch_path.with_suffix('.sas7bdat')
                        brhdata_df, meta = pyreadstat.read_sas7bdat(str(lkp_branch_path))
                        brhdata_df = pl.from_pandas(brhdata_df)
                        lkp_found = True
                        print(f"Read LKP_BRANCH as SAS dataset with {len(brhdata_df)} records")
                        break
                    except Exception as e:
                        print(f"Error reading as SAS: {e}")
                        continue
                else:
                    try:
                        with open(lkp_branch_path, 'r') as f:
                            brh_lines = []
                            lines = f.readlines()

                            first_line = lines[0].strip() if lines else ""
                            if 'BRANCH' in first_line.upper() or 'BRABBR' in first_line.upper():
                                start_idx = 1
                            else:
                                start_idx = 0

                            for line in lines[start_idx:]:
                                if line.strip():
                                    if '\t' in line:
                                        parts = line.strip().split('\t')
                                    elif ',' in line:
                                        parts = line.strip().split(',')
                                    else:
                                        parts = [p for p in line.strip().split(' ') if p]

                                    if parts and len(parts) >= 2:
                                        try:
                                            if parts[0].replace('.', '').isdigit():
                                                branch = float(parts[0]) if '.' in parts[0] else int(parts[0])
                                                brabbr = parts[1].strip()
                                            else:
                                                branch = float(parts[1]) if '.' in parts[1] else int(parts[1])
                                                brabbr = parts[0].strip()
                                            brh_lines.append({"BRANCH": branch, "BRABBR": brabbr})
                                        except (ValueError, IndexError):
                                            continue

                            if brh_lines:
                                brhdata_df = pl.DataFrame(brh_lines)
                                lkp_found = True
                                print(f"Read LKP_BRANCH as flat file with {len(brhdata_df)} records")
                                break
                    except Exception as e:
                        print(f"Error reading as flat file: {e}")
                        continue

        if not lkp_found or brhdata_df is None or brhdata_df.is_empty():
            print("LKP_BRANCH file not found or empty - using branch codes without abbreviation")
            unique_branches = hpacc_df["BRANCH"].unique().to_list()
            brh_lines = []
            for branch in unique_branches:
                if branch is not None:
                    if isinstance(branch, float) and branch.is_integer():
                        branch_int = int(branch)
                    else:
                        branch_int = branch
                    brh_lines.append({"BRANCH": branch, "BRABBR": f"BR{branch_int}"})
            brhdata_df = pl.DataFrame(brh_lines)
            print(f"Created default branch mapping with {len(brhdata_df)} records")

    except Exception as e:
        print(f"Error reading LKP_BRANCH: {e}")
        unique_branches = hpacc_df["BRANCH"].unique().to_list()
        brh_lines = []
        for branch in unique_branches:
            if branch is not None:
                if isinstance(branch, float) and branch.is_integer():
                    branch_int = int(branch)
                else:
                    branch_int = branch
                brh_lines.append({"BRANCH": branch, "BRABBR": f"BR{branch_int}"})
        brhdata_df = pl.DataFrame(brh_lines)
        print(f"Created default branch mapping with {len(brhdata_df)} records")

    if brhdata_df is not None and not brhdata_df.is_empty():
        brhdata_df = brhdata_df.with_columns(
            pl.col("BRANCH").cast(pl.Float64)
        )

        hpacc_df = hpacc_df.sort("BRANCH")
        brhdata_df = brhdata_df.sort("BRANCH")
        hpacc_df = hpacc_df.join(brhdata_df, on="BRANCH", how="left")
    else:
        hpacc_df = hpacc_df.with_columns(
            pl.lit("UNK").alias("BRABBR")
        )

    # 4. Merge with CIS
    hpcis_df = hpcis_df.sort("ACCTNO")
    hpacc_df = hpacc_df.sort("ACCTNO")
    hpacc_df = hpacc_df.join(hpcis_df, on="ACCTNO", how="inner")

    if hpacc_df.is_empty():
        print("No data after merging with CIS. Exiting.")
        return

    print(f"After CIS merge: {len(hpacc_df)} records")

    # 5. Summarize by BRANCH and ICNO
    hpacc_df = hpacc_df.sort(["BRANCH", "ICNO"])
    hpic_df = hpacc_df.group_by(["BRANCH", "ICNO"]).agg(
        pl.sum("BALANCE").alias("BALANCE_SUM")
    )

    # 6. Top 10 customers per branch
    hpic_df = hpic_df.sort(["BRANCH", "BALANCE_SUM"], descending=[False, True])

    hpic_df = hpic_df.with_columns(
        pl.int_range(1, pl.len() + 1).over("BRANCH").alias("N")
    ).filter(pl.col("N") <= 10)

    hpic_df = hpic_df.rename({"BALANCE_SUM": "TOTBAL"}).sort(["BRANCH", "ICNO"])
    hpacc_df = hpacc_df.sort(["BRANCH", "ICNO"])
    hpacc1_df = hpacc_df.join(hpic_df.select(["BRANCH", "ICNO", "TOTBAL"]),
                             on=["BRANCH", "ICNO"], how="inner")

    hpacc1_df = hpacc1_df.with_columns([
        pl.col("MTHARR").fill_null(0),
        pl.col("BALANCE").fill_null(0),
        pl.col("CUSTNAME").fill_null(""),
        pl.col("PRODUCT").fill_null(""),
        pl.col("BORSTAT").fill_null(""),
        pl.col("ISSUEDT").fill_null(0),
    ])

    hpacc1_df = hpacc1_df.sort(["BRANCH", "TOTBAL", "ICNO", "ACCTNO"],
                              descending=[False, True, False, False])

    # 7. Generate conventional bank report with production formatting
    generate_hp_report(hpacc1_df, rdate, output_path)

    print(f"Processing complete. Top {len(hpacc1_df)} records identified.")
    print(f"Report saved to: {output_path}")


# ---------------------------------------------------------------------------
# Report formatting constants - derived by measuring the actual production
# text output column-by-column. Every printed line is padded/truncated to
# LINE_WIDTH, and the first character of each line is the legacy ASA
# carriage-control character (' ' = single space, '0' = double space,
# '1' = start new page).
# ---------------------------------------------------------------------------
LINE_WIDTH = 130

ACCTNO_W = 12   # columns 1-12  (after the 1-char control column)
NOTENO_W = 7    # columns 13-19
NAME_W = 31     # columns 20-50 (name truncated to 30 chars, then padded to 31)
TYPE_W = 5      # columns 51-55
STATUS_W = 7    # columns 56-62
BALANCE_W = 18  # columns 63-80
MTHARR_W = 7    # right-justified value
MTHARR_GAP = 5  # spaces after the MTHARR value, before the date
DATE_W = 8      # columns 93-100


def generate_hp_report(df, rdate, output_path):
    """Generate formatted report matching production output exactly."""
    if df.is_empty():
        print("No data to report")
        return

    branches = df["BRANCH"].unique().to_list()

    def pad(line: str) -> str:
        return line[:LINE_WIDTH].ljust(LINE_WIDTH)

    with open(output_path, 'w') as f:
        page_num = 0

        for branch in branches:
            branch_df = df.filter(pl.col("BRANCH") == branch).sort(
                ["TOTBAL", "ICNO", "ACCTNO"], descending=[True, False, False]
            )

            page_num += 1

            # Branch code (3 digit, zero padded)
            if isinstance(branch, float) and branch.is_integer():
                branch_code = int(branch)
            else:
                branch_code = int(branch) if branch is not None else 0

            # New-page control line ('1' carriage control, blank content)
            f.write(pad("1") + "\n")

            # Page banner - exact spacing matched to production
            line1 = " PUBLIC BANK BERHAD" + " " * 61 + f"{rdate}  PAGE NO : {page_num}"
            f.write(pad(line1) + "\n")

            line2 = " TOP TEN LARGE ACCOUNTS FOR HPD (CONVENTIONAL & AITAB) AS AT " + rdate
            f.write(pad(line2) + "\n")

            f.write(pad(" REPORT ID: EIMHPTOP") + "\n")

            f.write(pad(f"0BRANCH CODE= {branch_code:03d}") + "\n")

            # Column headers - exact spacing from production
            f.write(pad("0            NOTE                                 LOAN  BORROWER                  MONTH      ISSUE") + "\n")
            f.write(pad(" MNI NO      NO     NAME                          TYPE  STATUS        NET BALANCE PASS DUE   DATE") + "\n")
            f.write(pad(" " + "-" * 104) + "\n")

            branch_total = 0
            icnos = branch_df["ICNO"].unique().to_list()

            for icno in icnos:
                ic_df = branch_df.filter(pl.col("ICNO") == icno)
                ic_total = 0

                for row in ic_df.iter_rows(named=True):
                    # ACCTNO - 12 chars, left aligned
                    acctno_raw = str(row.get('ACCTNO', '') or '')
                    acctno = acctno_raw.split('.')[0] if '.' in acctno_raw else acctno_raw
                    acctno = acctno.ljust(ACCTNO_W)

                    # NOTENO - 7 chars, left aligned
                    noteno_raw = str(row.get('NOTENO', '') or '')
                    noteno = noteno_raw.split('.')[0] if '.' in noteno_raw else noteno_raw
                    noteno = noteno.ljust(NOTENO_W)

                    # Customer name - truncate to 30 chars, pad to 31
                    custname = (str(row.get('CUSTNAME', '') or '')[:30]).ljust(NAME_W)

                    # Product type - 5 chars, left aligned
                    product_raw = str(row.get('PRODUCT', '') or '')
                    product = product_raw.split('.')[0] if '.' in product_raw else product_raw
                    product = product.ljust(TYPE_W)

                    # BORSTAT - 7 chars, left aligned
                    borstat = str(row.get('BORSTAT', '') or '').ljust(STATUS_W)

                    # NET BALANCE - right aligned, comma separated, 2 decimals
                    balance_val = row.get('BALANCE', 0) or 0
                    balance = f"{balance_val:{BALANCE_W},.2f}"

                    # PASS DUE (MTHARR) - right aligned then gap before date
                    mtharr_val = row.get('MTHARR', 0) or 0
                    mtharr = f"{int(mtharr_val):>{MTHARR_W}}" + " " * MTHARR_GAP

                    # ISSUE DATE - 8 chars, left aligned
                    issdate = " " * DATE_W
                    issuedt_val = row.get('ISSUEDT')
                    if issuedt_val:
                        try:
                            if isinstance(issuedt_val, (datetime, pl.Date)):
                                issdate = issuedt_val.strftime("%d%b%y").upper().ljust(DATE_W)
                            else:
                                sas_base = datetime(1960, 1, 1)
                                if isinstance(issuedt_val, (int, float)) and issuedt_val > 0:
                                    issdate = (sas_base + timedelta(days=int(issuedt_val))).strftime("%d%b%y").upper().ljust(DATE_W)
                        except Exception:
                            issdate = " " * DATE_W

                    detail_line = f" {acctno}{noteno}{custname}{product}{borstat}{balance}{mtharr}{issdate}"
                    f.write(pad(detail_line) + "\n")

                    ic_total += row.get('BALANCE', 0) or 0
                    branch_total += row.get('BALANCE', 0) or 0

                # ICNO total block - exact spacing matched to production
                f.write(pad(" " * 65 + "-" * 16) + "\n")
                f.write(pad(" " * 58 + f"TOTAL:{ic_total:17,.2f}") + "\n")
                f.write(pad(" " * 65 + "=" * 16) + "\n")

            # Branch total block - exact spacing matched to production
            f.write(pad(" " * 65 + "-" * 16) + "\n")
            f.write(pad(" " * 51 + f"BRANCH TOTAL:{branch_total:17,.2f}") + "\n")
            f.write(pad(" " * 65 + "=" * 16) + "\n")


if __name__ == "__main__":
    eimhptop()
