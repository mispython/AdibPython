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
import posixpath
import pyreadstat
import pandas as pd
import paramiko
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

INPUT_DIR           = Path('/exdwh/host/dp/cafd_sasdata')
OUTPUT_DIR          = Path('/dwh/ftp/drr')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DRR_HOST_DESC       = "Data Report Repository (DRR)"
DRR_REMOTE_DIR      = "WMTB-SA&S/DEP - SAS/MOVEMENT"

SFTP_CONTROL_FILE   = Path('/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat')

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
# ----------------------------------------------------------------------------
# This job runs on the 1st of each month and reports on the month that just
# closed. Report date = yesterday. When run on 1 Nov, that yields 31 Oct.
# The filename suffix follows SAS's %TOMM.%TOYY, which here resolves to the
# REPORT date's MMYY (e.g. "1026" for 31 Oct 2026), matching the JCL PUTs.
# ============================================================================

_run_day = datetime.now()
if _run_day.day != 1:
    print(f"WARNING: expected to run on day 1 of the month, "
          f"but today is day {_run_day.day}. "
          f"Report date will be {(_run_day - timedelta(days=1)):%Y-%m-%d}.")

reptdate = _run_day - timedelta(days=1)               # yesterday

reptyear = reptdate.strftime('%Y')
reptmon  = reptdate.strftime('%m')
reptday  = reptdate.strftime('%d')
rdate    = reptdate.strftime('%d/%m/%y')              # DDMMYY8.

FILENAME_DATE_SUFFIX = reptdate.strftime('%m%y')      # e.g. "1026" for 31 Oct 2026

print(f"Run day:     {_run_day:%Y-%m-%d}")
print(f"Report Date: {rdate}  (filename suffix: {FILENAME_DATE_SUFFIX})")


# ============================================================================
# FILENAME MAPPING
# ----------------------------------------------------------------------------
# One source of truth: the SAME filename is used for the local file on disk
# AND for the SFTP upload to DRR. Mirrors the DRR#SFTP JCL PUT statements:
#   CAFCYI@MMYY.TXT   <- SAP.PBB.CAMOVE.FCYI
#   CAFCYC@MMYY.TXT   <- SAP.PBB.CAMOVE.FCYC
#   ICAINDV@MMYY.TXT  <- SAP.PIBB.CAMOVE.INDV
#   CAINDV@MMYY.TXT   <- SAP.PBB.CAMOVE.INDV
#   ICACORP@MMYY.TXT  <- SAP.PIBB.CAMOVE.CORP
#   CACORP@MMYY.TXT   <- SAP.PBB.CAMOVE.CORP
#   FDFCYI@MMYY.TXT   <- SAP.PBB.FDMOVE.FCYI
#   FDFCYC@MMYY.TXT   <- SAP.PBB.FDMOVE.FCYC
#   IFDINDV@MMYY.TXT  <- SAP.PIBB.FDMOVE.INDV
#   FDINDV@MMYY.TXT   <- SAP.PBB.FDMOVE.INDV
#   IFDCORP@MMYY.TXT  <- SAP.PIBB.FDMOVE.CORP
#   FDCORP@MMYY.TXT   <- SAP.PBB.FDMOVE.CORP
# ============================================================================

_CATEGORY_PREFIX = {
    'CAMFYI': 'CAFCYI',
    'CAMFYC': 'CAFCYC',
    'CAMII':  'ICAINDV',
    'CAMIC':  'CAINDV',
    'CAMCI':  'ICACORP',
    'CAMCC':  'CACORP',
    'FDMFYI': 'FDFCYI',
    'FDMFYC': 'FDFCYC',
    'FDMII':  'IFDINDV',
    'FDMIC':  'FDINDV',
    'FDMCI':  'IFDCORP',
    'FDMCC':  'FDCORP',
}


def report_filename(category: str) -> str:
    """Return the JCL-style report filename for a category code,
    e.g. CAMFYI -> 'CAFCYI@1026.TXT' for report date 31 Oct 2026.

    The same string is used for BOTH the local file written to OUTPUT_DIR
    and the remote filename uploaded to DRR.
    """
    try:
        prefix = _CATEGORY_PREFIX[category]
    except KeyError as exc:
        raise KeyError(
            f"No filename prefix defined for category {category!r}. "
            f"Known categories: {sorted(_CATEGORY_PREFIX)}"
        ) from exc
    return f'{prefix}@{FILENAME_DATE_SUFFIX}.TXT'


# ============================================================================
# HELPERS
# ============================================================================

def read_sas(path: Path) -> pd.DataFrame:
    df, _meta = pyreadstat.read_sas7bdat(str(path))
    df.columns = [c.upper() for c in df.columns]
    return df


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
    """Writes one text report, mirroring the SAS FILE/PUT block.

    Opened with newline='\\r\\n' to match the SAS FTP step's text-mode
    line ending on the DRR side.
    """
    with open(out_path, 'w', newline='\r\n') as f:
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

def process_camv() -> list:
    path = INPUT_DIR / f'camv{reptday}{reptmon}.sas7bdat'
    df = read_sas(path)
    df = standardize_columns(df, [
        'BRANCH', 'PRODUCT', 'CUSTCODE', 'COSTCTR',
        'ACCTNO', 'NAME', 'CURBAL', 'PCURBAL', 'NETBALC',
    ])

    df['BRABV'] = df['BRANCH'].apply(fmt_branch)
    df['DDCUSTCD'] = df['CUSTCODE'].apply(ddcustcd_format)

    df = df[~df['PRODUCT'].isin([79, 80, 413])]
    df = df[~df['DDCUSTCD'].isin(EXCLUDED_DDCUSTCD_CODES)]

    def classify(row):
        is_fy_product = (
            400 <= row['PRODUCT'] <= 411
            or 420 <= row['PRODUCT'] <= 431
            or 432 <= row['PRODUCT'] <= 434
        )
        if is_fy_product:
            return 'CAMFYI' if row['CUSTCODE'] in ISLAMIC_CUSTCODES else 'CAMFYC'

        if row['CUSTCODE'] in ISLAMIC_CUSTCODES:
            return 'CAMII' if 3000 <= row['COSTCTR'] <= 3999 else 'CAMIC'

        if 3000 <= row['COSTCTR'] <= 3999:
            if not (3790000000 <= row['ACCTNO'] <= 3799999999):
                return 'CAMCI'
            return None
        else:
            if not (3590000000 <= row['ACCTNO'] <= 3599999999):
                return 'CAMCC'
            return None

    df['CATEGORY'] = df.apply(classify, axis=1)
    df = df[df['CATEGORY'].notna()]

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

    written = []
    for cat, (title, subtitle, subcol, include_net_total) in specs.items():
        sub = df[df['CATEGORY'] == cat]
        fname = report_filename(cat)
        out_path = OUTPUT_DIR / fname
        write_report(
            sub, out_path,
            title, subtitle, subcol,
            header_net_label='NETBALC',
            net_field='NETBALC',
            include_net_total=include_net_total,
        )
        written.append((out_path, fname))
        print(f"{cat}: {len(sub)} records -> {fname}")
    return written


# ============================================================================
# PROCESS FIXED DEPOSIT MOVEMENTS (FDMV)
# ============================================================================

def process_fdmv() -> list:
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

    written = []
    for cat, (title, subtitle, subcol, header_net_label) in specs.items():
        sub = df[df['CATEGORY'] == cat]
        fname = report_filename(cat)
        out_path = OUTPUT_DIR / fname
        write_report(
            sub, out_path,
            title, subtitle, subcol,
            header_net_label=header_net_label,
            net_field='NETBALF',
            include_net_total=False,
        )
        written.append((out_path, fname))
        print(f"{cat}: {len(sub)} records -> {fname}")
    return written


# ============================================================================
# SFTP UPLOAD
# ============================================================================

def get_sftp_info(host_desc: str):
    """Return (SFTP_ID, SFTP_PW, HOST_IP, HOST_KEY) for the row matching HOST_DESC."""
    if not SFTP_CONTROL_FILE.exists():
        raise FileNotFoundError(f"SFTP control file not found: {SFTP_CONTROL_FILE}")

    ctl, _meta = pyreadstat.read_sas7bdat(str(SFTP_CONTROL_FILE))
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]

    required = {"HOST_DESC", "SFTP_ID", "SFTP_PW", "HOST_IP"}
    missing_columns = sorted(required.difference(ctl.columns))
    if missing_columns:
        raise RuntimeError(
            "SFTP control file is missing columns: " + ", ".join(missing_columns)
        )

    descriptions = ctl["HOST_DESC"].fillna("").astype(str).str.strip()
    rows = ctl[descriptions.eq(host_desc)]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one SFTP row for '{host_desc}'; found {len(rows)}"
        )

    row = rows.iloc[0]

    def clean(value):
        return str(value).strip() if pd.notna(value) else ""

    sftp_id  = clean(row["SFTP_ID"])
    sftp_pw  = clean(row["SFTP_PW"])
    host_ip  = clean(row["HOST_IP"])
    host_key = clean(row["HOST_KEY"]) if "HOST_KEY" in ctl.columns else ""

    empty_values = [
        name for name, value in (
            ("SFTP_ID", sftp_id), ("SFTP_PW", sftp_pw), ("HOST_IP", host_ip)
        ) if not value
    ]
    if empty_values:
        raise RuntimeError(
            f"SFTP row for '{host_desc}' has blank values: {', '.join(empty_values)}"
        )

    return sftp_id, sftp_pw, host_ip, host_key


def sftp_upload(host_desc: str, remote_dir: str, local_file: Path,
                remote_name: str) -> None:
    """Upload one file to the SFTP server identified by HOST_DESC.

    Uses text-mode sftp.open(remote_name, 'w') instead of sftp.put() so the
    server negotiates line endings, mirroring the SAS FTP 'mode=text' step.
    """
    if not local_file.exists():
        raise FileNotFoundError(f"Local report missing: {local_file}")

    sftp_id, sftp_pw, host_ip, _host_key = get_sftp_info(host_desc)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to {host_desc} ({host_ip}) as {sftp_id} ...")
    try:
        ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    except Exception as exc:
        raise ConnectionError(
            f"Could not connect to {host_desc} ({host_ip}): {exc}"
        ) from exc

    try:
        sftp = ssh.open_sftp()
        try:
            try:
                sftp.chdir(remote_dir)
            except IOError as exc:
                raise FileNotFoundError(
                    f"Remote folder '{remote_dir}' not found on {host_desc}"
                ) from exc

            print(f"  SFTP upload (text mode): {local_file.name} -> "
                  f"{posixpath.join(remote_dir, remote_name)}")

            with open(local_file, 'r', newline='') as src:
                content = src.read()
            with sftp.open(remote_name, 'w') as dst:
                dst.write(content)

            remote_size = sftp.stat(remote_name).st_size
            local_size  = local_file.stat().st_size
            if remote_size != local_size:
                print(f"  NOTE: size differs for {remote_name} "
                      f"(local={local_size}, remote={remote_size}) - "
                      f"expected if the server translated newlines.")
        finally:
            sftp.close()
    finally:
        ssh.close()


def sftp_to_drr(file_pairs: list) -> None:
    """JCL step: FTP TO SAS DATAWAREHOUSE (DRR).

    Each element is a (local_path, remote_name) tuple. Since the local file
    and remote name are derived from the same report_filename() call, they
    are identical by construction.
    """
    print("\nUploading to DRR via SFTP...")
    for local_file, remote_name in file_pairs:
        sftp_upload(
            DRR_HOST_DESC,
            DRR_REMOTE_DIR,
            local_file,
            remote_name,
        )
    print("  DRR upload complete.")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    camv_files = process_camv()
    fdmv_files = process_fdmv()
    all_files = camv_files + fdmv_files

    print(f"\nCompleted: {len(all_files)} text reports generated in {OUTPUT_DIR}")
    for local_path, remote_name in all_files:
        print(f"  {local_path.name}  ->  {remote_name}")

    try:
        sftp_to_drr(all_files)
    except Exception as exc:
        print(f"\nSFTP upload FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
