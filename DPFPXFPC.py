"""
CAMV / FDMV Movement Reports
=============================
Python re-implementation of the original SAS program.

Mirrors the original SAS structure:
    %INC PGM(PBBDPFMT,PBBELF);
by importing the two format modules (pbbdpfmt.py, pbbelf.py) that must sit
alongside this script, instead of re-defining the lookup tables inline.

"""

import sys
import pyreadstat
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# Make sure the two format modules are importable regardless of the
# caller's working directory (they must live next to this script, mirroring
# how %INC PGM(PBBDPFMT,PBBELF); pulled them into the SAS job).
sys.path.insert(0, str(Path(__file__).resolve().parent))

import PBBELF      # PUT(BRANCH, BRCHCD.)
import PBBDPFMT     # PUT(CUSTCODE, DDCUSTCD.)

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / '/exdwh/host/dp/cafd_sasdata'
OUTPUT_DIR = BASE_DIR / '/dwh/ftp/drr'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DRR_HOST_DESC       = "Data Report Repository (DRR)"
DRR_REMOTE_DIR      = "WMTB-SA&S/DEP - SAS/MOVEMENT"

ISLAMIC_CUSTCODES = {77, 78, 95, 96}

# The literal exclusion list from the SAS source: "CUSTCD NOT IN
# (02,03,07,10,12,81,82,83,84)". Read as the 2-digit DDCUSTCD.-formatted
# codes - see the module docstring for why.
EXCLUDED_DDCUSTCD_CODES = {'02', '03', '07', '10', '12', '81', '82', '83', '84'}


def fmt_branch(branch_code):
    """PUT(BRANCH, BRCHCD.) via pbbelf.format_brchcd."""
    if branch_code is None or pd.isna(branch_code):
        return ''
    return PBBELF.format_brchcd(int(branch_code))


def ddcustcd_format(cust_code):
    """PUT(CUSTCODE, DDCUSTCD.) via pbbdpfmt.ddcustcd_format."""
    if cust_code is None or pd.isna(cust_code):
        return '79'
    return PBBDPFMT.ddcustcd_format(int(cust_code))


def fmt_num(x):
    """Render numbers the way SAS's default PUT would - no trailing '.0'
    for whole numbers, but keep decimals when present."""
    if pd.isna(x):
        return ""
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(x)


# ============================================================================
# REPORTING DATE
# (REPTDATE dataset removed - use "yesterday" via datetime/timedelta)
# ============================================================================

reptdate = datetime.now() - timedelta(days=7)
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = reptdate.strftime('%d/%m/%y')  # equivalent to SAS DDMMYY8.

print(f"Report Date: {rdate}")


# ============================================================================
# HELPERS
# ============================================================================

def read_sas(path: Path) -> pd.DataFrame:
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.upper() for c in df.columns]
    return df


# Column-name variants seen across SAS extracts, keyed by the standard name
# used throughout this script. If a dataset is missing a required column,
# the error message will show you the actual columns so you can add the
# real variant here.
COLUMN_ALIASES = {
    'BRANCH':   ['BRANCH', 'BRANCHNO', 'BRCH', 'BRCHNO'],
    'PRODUCT':  ['PRODUCT', 'PRODCODE', 'PROD', 'PRODUCTCODE'],
    'CUSTCODE': ['CUSTCODE', 'CUST_CODE', 'CUSTOMERCODE'],
    'COSTCTR':  ['COSTCTR', 'COST_CTR', 'COSTCENTRE', 'COSTCENTER'],
    'ACCTNO':   ['ACCTNO', 'ACCT_NO', 'ACCOUNTNO', 'ACCOUNTNUMBER'],
    'NAME':     ['NAME', 'CUSTNAME', 'CUST_NAME', 'ACCTNAME'],
    'CURBAL':   ['CURBAL', 'CUR_BAL'],
    'PCURBAL':  ['PCURBAL', 'PCUR_BAL', 'PREVCURBAL'],
    'NETBALC':  ['NETBALC', 'NETBAL_C', 'NETBAL'],
    'NETBALF':  ['NETBALF', 'NETBAL_F'],
}


def standardize_columns(df: pd.DataFrame, required: list) -> pd.DataFrame:
    """Renames whichever alias is present for each required standard column
    name. Raises a clear error (listing the dataset's real columns) if a
    required column can't be matched to any known alias."""
    df = df.copy()
    available = set(df.columns)
    rename_map = {}
    missing = []

    for std_name in required:
        aliases = COLUMN_ALIASES.get(std_name, [std_name])
        match = next((a for a in aliases if a in available), None)
        if match is None:
            missing.append(std_name)
        elif match != std_name:
            rename_map[match] = std_name

    if missing:
        raise KeyError(
            f"Could not find a match for required column(s) {missing}.\n"
            f"Columns actually present in this dataset: {sorted(df.columns)}\n"
            f"Add the real column name to COLUMN_ALIASES for the missing "
            f"field(s) above and re-run."
        )

    return df.rename(columns=rename_map)


def write_report(df: pd.DataFrame, out_path: Path, title: str, subtitle: str,
                  subtitle_col: int, header_net_label: str, net_field: str,
                  include_net_total: bool):
    """Writes one text report, mirroring the SAS FILE/PUT block:
       line 1: title @001, subtitle @<subtitle_col>
       line 2: AS AT <rdate>
       line 3: column header row
       body:   one detail line per record, fields joined as "value ;value ;..."
       last:   totals line, e.g. ";;;;<TCURBAL> ;<TCURBALP> ;<TNETBALC>"
    """
    with open(out_path, 'w') as f:
        header_line = title.ljust(subtitle_col - 1) + subtitle
        f.write(header_line + "\n")
        f.write(f"AS AT {rdate}\n")
        f.write(f"BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;{header_net_label};CUSTCODE\n")

        tcurbal = tcurbalp = tnetbal = 0.0
        for _, r in df.iterrows():
            curbal = r['CURBAL']
            pcurbal = r['PCURBAL']
            netbal = r[net_field]

            fields = [
                fmt_num(r['BRANCH']), r['BRABV'], r['NAME'], fmt_num(r['ACCTNO']),
                fmt_num(curbal), fmt_num(pcurbal), fmt_num(netbal), fmt_num(r['CUSTCODE']),
            ]
            f.write(" ;".join(fields) + "\n")

            tcurbal += 0 if pd.isna(curbal) else curbal
            tcurbalp += 0 if pd.isna(pcurbal) else pcurbal
            tnetbal += 0 if pd.isna(netbal) else netbal

        if include_net_total:
            f.write(f";;;;{fmt_num(tcurbal)} ;{fmt_num(tcurbalp)} ;{fmt_num(tnetbal)}\n")
        else:
            f.write(f";;;;{fmt_num(tcurbal)} ;{fmt_num(tcurbalp)}\n")


# ============================================================================
# PROCESS CURRENT ACCOUNT MOVEMENTS (CAMV)
# ============================================================================

def process_camv():
    path = INPUT_DIR / f'camv{reptday}{reptmon}.sas7bdat'
    df = read_sas(path)
    df = standardize_columns(df, [
        'BRANCH', 'PRODUCT', 'CUSTCODE', 'COSTCTR',
        'ACCTNO', 'NAME', 'CURBAL', 'PCURBAL', 'NETBALC',
    ])

    df['BRABV'] = df['BRANCH'].apply(fmt_branch)
    df['DDCUSTCD'] = df['CUSTCODE'].apply(ddcustcd_format)

    # Subsetting IF: drop excluded products / customer codes up front.
    df = df[~df['PRODUCT'].isin([79, 80, 413])]
    df = df[~df['DDCUSTCD'].isin(EXCLUDED_DDCUSTCD_CODES)]

    def classify(row):
        # NOTE: the SAS source also requires CURCODE NE 'MYR' here, but this
        # dataset has no CURCODE column at all (confirmed against the real
        # extract), so the FYI/FYC split is done on PRODUCT range alone.
        is_fy_product = (
            400 <= row['PRODUCT'] <= 411
            or 420 <= row['PRODUCT'] <= 431
            or 432 <= row['PRODUCT'] <= 434
        )
        if is_fy_product:
            return 'CAMFYI' if row['CUSTCODE'] in ISLAMIC_CUSTCODES else 'CAMFYC'

        if row['CUSTCODE'] in ISLAMIC_CUSTCODES:
            return 'CAMII' if 3000 <= row['COSTCTR'] <= 3999 else 'CAMIC'

        # non-Islamic-code, non-FY records
        if 3000 <= row['COSTCTR'] <= 3999:
            if not (3790000000 <= row['ACCTNO'] <= 3799999999):
                return 'CAMCI'
            return None  # matches implicit SAS drop (no ELSE branch)
        else:
            if not (3590000000 <= row['ACCTNO'] <= 3599999999):
                return 'CAMCC'
            return None  # matches implicit SAS drop (no ELSE branch)

    df['CATEGORY'] = df.apply(classify, axis=1)
    df = df[df['CATEGORY'].notna()]

    # (title, subtitle, subtitle start column, include NETBALC total row)
    specs = {
        'CAMFYI': ('CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (FOREIGN CURRENCY) INDIVIDUAL', 58, True),
        'CAMFYC': ('CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (FOREIGN CURRENCY) CORPORATE', 58, True),
        'CAMII':  ('CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (INDIVIDUAL CUSTOMERS - ISLAMIC)', 58, False),
        'CAMIC':  ('CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (INDIVIDUAL CUSTOMERS-CONVENTIONAL)', 58, False),
        'CAMCI':  ('CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (CORPORATE CUSTOMERS - ISLAMIC)', 58, False),
        'CAMCC':  ('CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (CORPORATE CUSTOMERS - CONVENTIONAL)', 58, False),
    }

    for cat, (title, subtitle, subcol, include_net_total) in specs.items():
        sub = df[df['CATEGORY'] == cat]
        write_report(
            sub, OUTPUT_DIR / f'{cat.lower()}.txt',
            title, subtitle, subcol,
            header_net_label='NETBALC',
            net_field='NETBALC',
            include_net_total=include_net_total,
        )
        print(f"{cat}: {len(sub)} records")


# ============================================================================
# PROCESS FIXED DEPOSIT MOVEMENTS (FDMV)
# ============================================================================

def process_fdmv():
    path = INPUT_DIR / f'fdmv{reptday}{reptmon}.sas7bdat'
    df = read_sas(path)
    df = standardize_columns(df, [
        'BRANCH', 'PRODUCT', 'CUSTCODE', 'COSTCTR',
        'ACCTNO', 'NAME', 'CURBAL', 'PCURBAL', 'NETBALF',
    ])

    df['BRABV'] = df['BRANCH'].apply(fmt_branch)

    def classify(row):
        if 350 <= row['PRODUCT'] <= 362:
            return 'FDMFYI' if row['CUSTCODE'] in ISLAMIC_CUSTCODES else 'FDMFYC'

        if row['CUSTCODE'] in ISLAMIC_CUSTCODES:
            return 'FDMII' if 3000 <= row['COSTCTR'] <= 3999 else 'FDMIC'

        return 'FDMCI' if 3000 <= row['COSTCTR'] <= 3999 else 'FDMCC'

    df['CATEGORY'] = df.apply(classify, axis=1)

    # (title, subtitle, subtitle start column, header net-column label)
    # NOTE: FDMFYI/FDMFYC header label of "NETBALC" (instead of NETBALF)
    # replicates a mismatch present in the original SAS PUT statement.
    specs = {
        'FDMFYI': ('FIXED DEPOSIT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (FOREIGN CURRENCY) INDIVIDUAL', 58, 'NETBALC'),
        'FDMFYC': ('FIXED DEPOSIT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (FOREIGN CURRENCY) CORPORATE', 58, 'NETBALC'),
        'FDMII':  ('FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (INDIVIDUAL CUSTOMERS - ISLAMIC)', 58, 'NETBALF'),
        'FDMIC':  ('FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (INDIVIDUAL CUSTOMERS - CONVENTIONAL)', 54, 'NETBALF'),
        'FDMCI':  ('FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (CORPORATE CUSTOMERS - ISLAMIC)', 54, 'NETBALF'),
        'FDMCC':  ('FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT',
                   'BY BRANCH (CORPORATE CUSTOMERS - CONVENTIONAL)', 54, 'NETBALF'),
    }

    for cat, (title, subtitle, subcol, header_net_label) in specs.items():
        sub = df[df['CATEGORY'] == cat]
        write_report(
            sub, OUTPUT_DIR / f'{cat.lower()}.txt',
            title, subtitle, subcol,
            header_net_label=header_net_label,
            net_field='NETBALF',
            include_net_total=False,
        )
        print(f"{cat}: {len(sub)} records")

def sftp_upload(host_desc: str, remote_dir: str, local_file: Path, remote_name: str) -> None:
    """Upload one file to the SFTP server identified by HOST_DESC."""
    sftp_id, sftp_pw, host_ip, _host_key = get_sftp_info(host_desc)

    ssh = paramiko.SSHClient()
    # HOST_KEY from the control file is not used for pinning (same as EIWFRMCR);
    # AutoAddPolicy is used until the stored key format is confirmed.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to {host_desc} ({host_ip}) as {sftp_id} ...")
    ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    try:
        sftp = ssh.open_sftp()
        try:
            try:
                sftp.chdir(remote_dir)
            except IOError as exc:
                raise FileNotFoundError(
                    f"Remote folder '{remote_dir}' not found on {host_desc}"
                ) from exc
            print(f"  SFTP upload: {local_file.name} -> {remote_dir}/{remote_name}")
            sftp.put(str(local_file), remote_name)
        finally:
            sftp.close()
    finally:
        ssh.close()


def sftp_to_drr() -> None:
    """JCL step: FTP TO SAS DATAWAREHOUSE (DRR)."""
    print("\nUploading to DRR via SFTP...")
    sftp_upload(DRR_HOST_DESC, DRR_REMOTE_DIR, OUTPUT_FILE, REMOTE_FILENAME)
    print("  DRR upload complete.")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    process_camv()
    process_fdmv()
    print(f"\nCompleted: 12 text reports generated in {OUTPUT_DIR}")


please help adjust and modify the codes:

i would like to add output to drr server. already add the functions, just need to make sure it aligns and workable
add import paramiko
