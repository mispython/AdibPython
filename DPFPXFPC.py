"""
kalmliq.py - Treasury (K1TBL / K3TBL) processing for BNM LCR reporting.

This is a direct port of the KALMLIQ SAS include (%INC PGM(KALMLIQ)),
kept as its own module - exactly like the original SAS keeps it as a
separate include rather than inline in EIBDLCRM.py - so it can be
tested, read, and maintained independently of the rest of the report.

=====================================================================
KNOWN GAPS (things referenced in the SAS source that are NOT
reconstructable from what's been shared so far):

1. K1TBX - the KTBL/KTBLALL step does:
       SET K1TBL(IN=A) K3TBL(IN=B) K1TBX;
   K1TBX is a THIRD dataset stacked in alongside K1TBL/K3TBL that is
   never defined in the KALMLIQ excerpt we have. It's presumably
   produced by one of the two %INC calls below. Until we see that
   source, treasury records from K1TBX are simply missing from this
   port's output.

2. %INC PGM(KAMLIQX);  -- runs between the K1TBL and K3TBL DATA steps.
3. %INC PGM(KALMLIQ4); -- runs after the K3TBL DATA step, before KTBL.
   Neither of these includes has been provided, so whatever additional
   filtering/derivation/output they do to K1TBL/K3TBL (or to build
   K1TBX) is not reflected here.

4. The later "DISTRIBUTION PROFILE OF CUSTOMER DEPOSITS (PART 3)"
   block (NON-INTERBANK REPOS / NON-INTERBANK NIDS, re-reading
   BNMK.K1TBL/K3TBL into a CAT/NAME/AMOUNT summary) is a separate
   report section that does NOT feed into KTBLALL/the main LCR figures.
   It is not implemented here since it's a distinct output, not part of
   the treasury amount that flows into the LCR consolidation. Let me
   know if you need it and I'll add it as its own function.

If you can get hold of KAMLIQX / KALMLIQ4 / the K1TBX source, send them
over and I'll fold them in properly instead of leaving this gap.
=====================================================================
"""

import os
import glob

from common import (
    read_sas_file,
    warn_missing_columns,
    debug_directory,
    sas_date_to_pydate,
    calculate_remaining_months,
    format_mth_bucket,
)
from config import PATHS, inst


# =============================================================================
# FILE DISCOVERY
# =============================================================================
def find_k1tbl_file(rep_date):
    """Find K1TBL file (BNMK.K1TBL&REPTMON&NOWK) with debugging."""
    base_path = PATHS['bnmk']
    month = rep_date['mon']
    week = rep_date['nowk']

    print(f"  Looking for K1TBL file...")
    print(f"    Base path: {base_path}")
    print(f"    Month: {month}, Week: {week}")

    if not os.path.exists(base_path):
        print(f"    ERROR: Directory does not exist: {base_path}")
        return None

    possible_names = [
        f"k1tbl{month}{week}.sas7bdat",
        f"K1TBL{month}{week}.sas7bdat",
        f"k1tbl{month}0{week}.sas7bdat",
        f"K1TBL{month}0{week}.sas7bdat",
        f"k1tbl{month}.sas7bdat",
        f"K1TBL{month}.sas7bdat",
    ]

    print(f"    Looking for exact matches:")
    for name in possible_names:
        full_path = os.path.join(base_path, name)
        exists = os.path.exists(full_path)
        print(f"      {name}: {'✓ Found' if exists else '✗ Not found'}")
        if exists:
            return full_path

    print(f"    Searching with wildcards:")
    wildcards = [
        f"*k1tbl*{month}*.sas7bdat",
        f"*K1TBL*{month}*.sas7bdat",
        f"*k1tbl*.sas7bdat",
        f"*K1TBL*.sas7bdat",
    ]

    for wildcard in wildcards:
        pattern = os.path.join(base_path, wildcard)
        matches = glob.glob(pattern)
        if matches:
            print(f"      {wildcard}: Found {len(matches)} file(s)")
            for m in matches[:5]:
                print(f"        - {os.path.basename(m)}")
            return matches[0]

    print(f"    No K1TBL files found. Listing directory contents:")
    debug_directory(base_path, pattern="k1")

    return None


def find_k3tbl_file(rep_date):
    """Find K3TBL file (BNMK.K3TBL&REPTMON&NOWK) with debugging."""
    base_path = PATHS['bnmk']
    month = rep_date['mon']
    week = rep_date['nowk']

    print(f"  Looking for K3TBL file...")
    print(f"    Base path: {base_path}")
    print(f"    Month: {month}, Week: {week}")

    if not os.path.exists(base_path):
        print(f"    ERROR: Directory does not exist: {base_path}")
        return None

    possible_names = [
        f"k3tbl{month}{week}.sas7bdat",
        f"K3TBL{month}{week}.sas7bdat",
        f"k3tbl{month}0{week}.sas7bdat",
        f"K3TBL{month}0{week}.sas7bdat",
        f"k3tbl{month}.sas7bdat",
        f"K3TBL{month}.sas7bdat",
    ]

    print(f"    Looking for exact matches:")
    for name in possible_names:
        full_path = os.path.join(base_path, name)
        exists = os.path.exists(full_path)
        print(f"      {name}: {'✓ Found' if exists else '✗ Not found'}")
        if exists:
            return full_path

    print(f"    Searching with wildcards:")
    wildcards = [
        f"*k3tbl*{month}*.sas7bdat",
        f"*K3TBL*{month}*.sas7bdat",
        f"*k3tbl*.sas7bdat",
        f"*K3TBL*.sas7bdat",
    ]

    for wildcard in wildcards:
        pattern = os.path.join(base_path, wildcard)
        matches = glob.glob(pattern)
        if matches:
            print(f"      {wildcard}: Found {len(matches)} file(s)")
            for m in matches[:5]:
                print(f"        - {os.path.basename(m)}")
            return matches[0]

    print(f"    No K3TBL files found. Listing directory contents:")
    debug_directory(base_path, pattern="k3")

    return None


# =============================================================================
# K1TBL - direct port of:
#   DATA K1TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT GWCCY
#                    GWSHN GWC2R GWDLP GWDLR);
#      SET BNMK.K1TBL&REPTMON&NOWK (RENAME=(GWMDT=MATDT GWBALC=AMOUNT
#                                           GWSDT=ISSDT));
#      IF GWMVT = 'P';
#      IF GWOCY IN ('XAU','XAT') OR GWCCY IN ('XAU','XAT') THEN DELETE;
#      ... (see kalmliq.sas for full logic)
# =============================================================================
def process_k1tbl(rep_date):
    """Process K1TBL from BNMK.K1TBL{REPTMON}{NOWK}"""
    records = []

    try:
        k1_filepath = find_k1tbl_file(rep_date)

        if k1_filepath is None:
            print(f"  No K1TBL file found")
            return records

        print(f"  Using K1TBL file: {k1_filepath}")
        df = read_sas_file(k1_filepath)  # columns normalized to lowercase

        if df is None:
            return records

        print(f"  Processing K1TBL with {len(df)} rows...")
        print(f"    Columns ({len(df.columns)}): {df.columns}")

        warn_missing_columns(
            df,
            ['gwmvt', 'gwccy', 'gwocy', 'gwmvts', 'gwctp', 'gwdlp', 'gwmdt',
             'gwsdt', 'gwbalc', 'gwshn', 'gwc2r', 'gwdlr'],
            'K1TBL'
        )

        gwmvt_col = 'gwmvt' if 'gwmvt' in df.columns else None
        if gwmvt_col is None:
            print(f"    Column 'gwmvt' not found! Available columns: {df.columns}")
            return records

        unique_gwmvt = df[gwmvt_col].unique().to_list()
        print(f"    Unique values in GWMVT: {unique_gwmvt}")

        gwmvt_values = df[gwmvt_col].to_list()
        p_count = sum(1 for v in gwmvt_values if str(v).upper() == 'P')
        print(f"    Rows with GWMVT = 'P': {p_count}")

        if p_count == 0:
            print(f"    No rows with GWMVT = 'P'. Sample values: {gwmvt_values[:10]}")
            return records

        print(f"    Sample rows (first 3):")
        sample_rows = df.head(3).rows(named=True)
        for i, row in enumerate(sample_rows):
            print(f"      Row {i+1}:")
            for key in ['gwmvt', 'gwccy', 'gwocy', 'gwmvts', 'gwctp', 'gwdlp', 'gwmdt', 'gwbalc']:
                if key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        total_rows = 0
        filtered_out = 0
        gwmvt_p = 0
        excluded_currency = 0
        item_assigned = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            gwmvt = str(row.get(gwmvt_col, '') or '').upper()

            # IF GWMVT = 'P';
            if gwmvt != 'P':
                filtered_out += 1
                continue
            gwmvt_p += 1

            gwccy = str(row.get('gwccy', '') or '').upper() if 'gwccy' in df.columns else ''
            gwocy = str(row.get('gwocy', '') or '').upper() if 'gwocy' in df.columns else ''

            # IF GWOCY='XAU' THEN DELETE; IF GWCCY='XAU' THEN DELETE;
            # IF GWOCY='XAT' THEN DELETE; IF GWCCY='XAT' THEN DELETE;
            if gwocy in ['XAU', 'XAT'] or gwccy in ['XAU', 'XAT']:
                excluded_currency += 1
                continue

            gwmvts = str(row.get('gwmvts', '') or '').upper() if 'gwmvts' in df.columns else ''
            gwctp = str(row.get('gwctp', '') or '').upper() if 'gwctp' in df.columns else ''
            gwdlp = str(row.get('gwdlp', '') or '').upper() if 'gwdlp' in df.columns else ''

            # RENAME=(GWMDT=MATDT GWBALC=AMOUNT GWSDT=ISSDT)
            matdt = sas_date_to_pydate(row.get('gwmdt')) if 'gwmdt' in df.columns else None
            issdt = sas_date_to_pydate(row.get('gwsdt')) if 'gwsdt' in df.columns else None
            amount = (row.get('gwbalc', 0) or 0) if 'gwbalc' in df.columns else 0
            gwshn = (row.get('gwshn', '') or '') if 'gwshn' in df.columns else ''
            gwc2r = (row.get('gwc2r', 0) or 0) if 'gwc2r' in df.columns else 0
            gwdlr = (row.get('gwdlr', '') or '') if 'gwdlr' in df.columns else ''

            if gwccy == 'MYR':
                # ----- PART = '95' branch -----
                part = '95'
                amtusd = 0
                amtsgd = 0

                if gwmvts == 'M':
                    # IF GWDLP IN ('BCD','BCI','BCS','BCQ','BCT','BCW','BQD') THEN ITEM='830'
                    if gwdlp in ['BCD', 'BCI', 'BCS', 'BCQ', 'BCT', 'BCW', 'BQD']:
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': '830', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })

                    # IF SUBSTR(GWCTP,1,1) = 'B' THEN SELECT (GWDLP) ...
                    if gwctp[:1] == 'B':
                        if gwdlp in ['LO', 'LC', 'LF', 'LS', 'LOI', 'LSI', 'LSC', 'LSW',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOC', 'LOW']:
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '610', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BO', 'BF', 'BOI', 'BFI', 'BSC', 'BSW', 'BOC', 'BOW']:
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        # OTHERWISE; -> no output

                    # SELECT (SUBSTR(GWDLP,2,2)) - independent of the GWCTP check above
                    dlp23 = gwdlp[1:3] if len(gwdlp) >= 2 else ''
                    if dlp23 in ['MI', 'MT']:
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': '820', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                    elif dlp23 in ['XI', 'XT']:
                        item_assigned += 1
                        records.append({
                            'part': part, 'item': '620', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                # (the FXS/FXO/... block is commented out in the SAS source - not ported)

            else:
                # ----- PART = '96' branch (foreign currency) -----
                part = '96'
                amtusd = amount if gwccy == 'USD' else 0
                amtsgd = amount if gwccy == 'SGD' else 0

                if gwmvts == 'M':
                    if gwctp[:1] == 'B' and gwctp != 'BW':
                        if gwdlp in ['LO', 'LC', 'LS', 'LF', 'LOI', 'LSI', 'LSC', 'LOC',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOW', 'LSW']:
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '610', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BC', 'BF', 'BO', 'BSC', 'BOW', 'BSW']:
                            # IF SUBSTR(GWSHN,1,6) ^= 'FCY-FD' THEN ITEM='810'
                            if gwshn[:6] != 'FCY-FD':
                                item_assigned += 1
                                records.append({
                                    'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                    'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                    'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                    'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                                })
                        elif gwdlp == 'BOC':
                            item_assigned += 1
                            records.append({
                                'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwshn': gwshn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        # OTHERWISE; -> no output
                # (the FXS/FXO/... block is commented out in the SAS source - not ported)

        print(f"  K1TBL processing stats:")
        print(f"    Total rows: {total_rows}")
        print(f"    Filtered out (GWMVT != 'P'): {filtered_out}")
        print(f"    Passed GWMVT = 'P': {gwmvt_p}")
        print(f"    Excluded (XAU/XAT currency): {excluded_currency}")
        print(f"    Records with item assigned: {item_assigned}")

    except Exception as e:
        print(f"  K1TBL warning: {e}")
        import traceback
        traceback.print_exc()

    return records


# =============================================================================
# K3TBL - direct port of:
#   DATA K3TBL (KEEP=PART ITEM MATDT AMOUNT AMTUSD AMTSGD ISSDT UTCCY
#                    UTCUS UTCTP UTSTY UTDLR UTDLP);
#      RETAIN PART '95';
#      SET BNMK.K3TBL&REPTMON&NOWK;
#      ... (see kalmliq.sas for full logic)
#
# NOTE: unlike K1TBL, K3TBL's source table already has native MATDT/ISSDT
# columns (no RENAME needed) - confirmed against the real file's column
# dump ('matdt', 'issdt' present directly).
# =============================================================================
def process_k3tbl(rep_date):
    """Process K3TBL from BNMK.K3TBL{REPTMON}{NOWK}"""
    records = []

    try:
        k3_filepath = find_k3tbl_file(rep_date)

        if k3_filepath is None:
            print(f"  No K3TBL file found")
            return records

        print(f"  Using K3TBL file: {k3_filepath}")
        df = read_sas_file(k3_filepath)  # columns normalized to lowercase

        if df is None:
            return records

        print(f"  Processing K3TBL with {len(df)} rows...")
        print(f"    Columns ({len(df.columns)}): {df.columns}")

        warn_missing_columns(
            df,
            ['utref', 'utsty', 'utdlp', 'utcus', 'utclc', 'utctp', 'matdt',
             'issdt', 'utamoc', 'utdpf', 'utccy', 'utdlr', 'utaict', 'utpcp',
             'utdpey', 'utdpe', 'utaicy', 'utait', 'utmm1'],
            'K3TBL'
        )

        utref_col = 'utref' if 'utref' in df.columns else None
        if utref_col:
            unique_utref = df[utref_col].unique().to_list()
            print(f"    Unique values in UTREF: {unique_utref[:20]}")
        else:
            print(f"    Column 'utref' not found!")

        utsty_col = 'utsty' if 'utsty' in df.columns else None
        if utsty_col:
            unique_utsty = df[utsty_col].unique().to_list()
            print(f"    Unique values in UTSTY: {unique_utsty[:20]}")

        matdt_col = 'matdt' if 'matdt' in df.columns else None
        issdt_col = 'issdt' if 'issdt' in df.columns else None
        if matdt_col is None:
            print("    !! WARNING [K3TBL]: no maturity date column found - "
                  "all K3TBL records will be dropped in build_ktblall().")

        print(f"    Sample rows (first 3):")
        sample_rows = df.head(3).rows(named=True)
        for i, row in enumerate(sample_rows):
            print(f"      Row {i+1}:")
            for key in ['utref', 'utsty', 'utdlp', 'utcus', 'utctp', 'matdt', 'utamoc', 'utdpf']:
                if key in df.columns:
                    print(f"        {key}: {row.get(key, 'N/A')}")

        total_rows = 0
        utref_match = 0
        item_assigned = 0
        matdt_missing = 0

        for row in df.iter_rows(named=True):
            total_rows += 1

            # AMOUNT = UTAMOC - UTDPF; IF UTSTY='IDC' THEN AMOUNT=UTAMOC + UTDPF;
            utamoc = (row.get('utamoc', 0) or 0) if 'utamoc' in df.columns else 0
            utdpf = (row.get('utdpf', 0) or 0) if 'utdpf' in df.columns else 0
            utsty = str(row.get(utsty_col, '') or '').upper() if utsty_col else ''
            amount = (utamoc + utdpf) if utsty == 'IDC' else (utamoc - utdpf)

            # IF &INST='PBB' THEN ... (inst is always 'PBB' here per config.py)
            utccy = str(row.get('utccy', 'MYR') or 'MYR').upper() if 'utccy' in df.columns else 'MYR'
            amtusd = amount if (inst == 'PBB' and utccy == 'USD') else 0
            amtsgd = amount if (inst == 'PBB' and utccy == 'SGD') else 0

            utcus = row.get('utcus', '') if 'utcus' in df.columns else ''
            utctp = row.get('utctp', 0) if 'utctp' in df.columns else 0
            utdlr = (row.get('utdlr', '') or '') if 'utdlr' in df.columns else ''
            utdlp = str(row.get('utdlp', '') or '').upper() if 'utdlp' in df.columns else ''
            utref = str(row.get(utref_col, '') or '').upper() if utref_col else ''
            utaict = (row.get('utaict', 0) or 0) if 'utaict' in df.columns else 0
            utpcp = (row.get('utpcp', 0) or 0) if 'utpcp' in df.columns else 0
            utdpey = (row.get('utdpey', 0) or 0) if 'utdpey' in df.columns else 0
            utdpe = (row.get('utdpe', 0) or 0) if 'utdpe' in df.columns else 0
            utaicy = (row.get('utaicy', 0) or 0) if 'utaicy' in df.columns else 0
            utait = (row.get('utait', 0) or 0) if 'utait' in df.columns else 0
            utmm1 = str(row.get('utmm1', '') or '').upper() if 'utmm1' in df.columns else ''

            matdt = sas_date_to_pydate(row.get(matdt_col)) if matdt_col else None
            # FIX vs previous port: ISSDT was never extracted for K3TBL,
            # so ORI30D was hardcoded to 0 downstream. The SAS KEEP list
            # includes ISSDT and KTBLALL computes ORI30D for K3TBL exactly
            # like it does for K1TBL (MATDT - ISSDT).
            issdt = sas_date_to_pydate(row.get(issdt_col)) if issdt_col else None
            if matdt is None:
                matdt_missing += 1

            part = '95'  # RETAIN PART '95';
            item = None

            def emit(it, amt):
                records.append({
                    'part': part, 'item': it, 'matdt': matdt, 'issdt': issdt,
                    'amount': amt, 'amtusd': amtusd, 'amtsgd': amtsgd,
                    'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                    'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                })

            # IF UTREF IN ('INV','DRI','DLG','AFSLIQ','AFSBOND','IAFSLIQ','AFS','IAFS') THEN DO;
            if utref in ['INV', 'DRI', 'DLG', 'AFSLIQ', 'AFSBOND', 'IAFSLIQ', 'AFS', 'IAFS']:
                utref_match += 1
                if utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGS', 'MTB', 'BNB', 'BNN',
                             'ITB', 'SAC', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM', 'CMB', 'MGI', 'SMC']:
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('631', amt)
                elif utsty == 'SDC':
                    amt = (utamoc * (utpcp / 100)) + utdpey + utdpe if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty == 'LDC':
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty in ['SLD', 'SSD']:
                    amt = (utamoc * (utpcp / 100)) + utaicy + utait if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty in ['SFD', 'SZD']:
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('632', amt)
                elif utsty == 'SBA':
                    if utdlp not in ['MOS', 'MSS']:
                        item_assigned += 1
                        emit('633', amount)
                elif utsty in ['ISB', 'DHB', 'KHA', 'PNB']:
                    item_assigned += 1
                    emit('636', amount)
                elif utsty == 'IDS':
                    item_assigned += 1
                    emit('635', amount)
                elif utsty == 'DBD':
                    # NOTE: SAS has WHEN('DBD')->'634' listed BEFORE
                    # WHEN('DMB','DBD','GRL','MTL','RUL')->'635'. SELECT/WHEN
                    # stops at the first match, so DBD always resolves to
                    # '634' here; the second WHEN's 'DBD' is unreachable.
                    item_assigned += 1
                    emit('634', amount)
                elif utsty in ['DMB', 'GRL', 'MTL', 'RUL']:
                    item_assigned += 1
                    emit('635', amount)
                elif utsty == 'PBA':
                    if utdlp in ['MOS', 'MSS']:
                        item_assigned += 1
                        emit('850', amount)
                # OTHERWISE; -> no output

            # ELSE IF UTREF IN ('PFD','PLD','PSD','PZD','PDC') THEN DO;
            elif utref in ['PFD', 'PLD', 'PSD', 'PZD', 'PDC']:
                utref_match += 1
                if utsty in ['IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP']:
                    item_assigned += 1
                    emit('840', amount)

            # ELSE IF UTREF IN ('IINV','IDRI','IDLG') THEN DO;
            elif utref in ['IINV', 'IDRI', 'IDLG']:
                utref_match += 1
                if utsty == 'SBA' and utdlp == 'IOP':
                    item_assigned += 1
                    emit('633', amount)
                elif utsty in ['SDC', 'LDC']:
                    item_assigned += 1
                    emit('632', amount)
                elif utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGI', 'ITB', 'SAC', 'BMN',
                               'BMC', 'BMF', 'SCD', 'SCM', 'MGS', 'MTB', 'BNB', 'BNN', 'CMB', 'SMC']:
                    amt = amount + utaict if inst == 'PBB' else amount
                    item_assigned += 1
                    emit('631', amt)
                elif utsty in ['ISB', 'IDS', 'IBZ', 'ICN']:
                    # FIX vs previous port:
                    #   SAS: IF UTMM1='GGB' THEN ITEM='636';
                    #        ELSE IF UTMM1='NGB' THEN ITEM='635';
                    #        AMOUNT = AMOUNT + UTAICT; OUTPUT;
                    # (previous port incorrectly checked for 'GGB' vs
                    # anything-else->'635'; the real else-condition is
                    # specifically 'NGB'. If neither matches, SAS would
                    # retain whatever ITEM held from a prior loop
                    # iteration - an edge case we don't replicate; we
                    # simply skip emitting a record if utmm1 isn't
                    # GGB/NGB, since a genuinely blank ITEM would be
                    # dropped downstream anyway by "IF ITEM ^= ' '".
                    if utmm1 == 'GGB':
                        item_assigned += 1
                        emit('636', amount + utaict)
                    elif utmm1 == 'NGB':
                        item_assigned += 1
                        emit('635', amount + utaict)
                elif utsty in ['DHB', 'KHA']:
                    item_assigned += 1
                    emit('636', amount)
                elif utsty == 'DBD':
                    item_assigned += 1
                    emit('634', amount)

            # IF UTSTY IN ('SIP') THEN DO; ITEM='610'; OUTPUT; END;
            # (this check is UNCONDITIONAL - outside/after the UTREF if/elif
            # chain above, exactly as in the SAS source - so it can fire in
            # addition to one of the branches above for the same row)
            if utsty == 'SIP':
                item_assigned += 1
                emit('610', amount)

        print(f"  K3TBL processing stats:")
        print(f"    Total rows: {total_rows}")
        print(f"    Rows matching UTREF patterns: {utref_match}")
        print(f"    Records with item assigned: {item_assigned}")
        print(f"    Records with matdt missing/None (will be dropped by build_ktblall): {matdt_missing}")

    except Exception as e:
        print(f"  K3TBL warning: {e}")
        import traceback
        traceback.print_exc()

    return records


# =============================================================================
# KTBLALL - direct port of:
#   DATA KTBL (KEEP=BNMCODE AMOUNT AMTUSD AMTSGD) KTBLALL;
#      SET K1TBL(IN=A) K3TBL(IN=B) K1TBX;    <- K1TBX not available, see module docstring
#      IF ITEM ^= ' ';
#      IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1; ELSE %REMMTH;
#      IF MATDT - ISSDT    < 8 THEN ORI30D = 0.1; ELSE ORI30D = (MATDT-ISSDT)/30;
#      BNMCODE = PART||ITEM||'00'||PUT(REMMTH,REMFMT.)||'0000Y';
#      OUTPUT;
#      IF PART = '95' THEN SUBSTR(BNMCODE,1,2) = '93'; ELSE SUBSTR(BNMCODE,1,2)='94';
#      OUTPUT;
# =============================================================================
def build_ktblall(k1_records, k3_records, rep_date):
    """
    Build KTBLALL from K1 and K3 records. Applies identically to both
    sources (both are normalized to have part/item/matdt/issdt/amount by
    the time they get here) - matching how the SAS KTBLALL step treats
    the stacked K1TBL+K3TBL(+K1TBX) the same way regardless of source.
    """
    all_records = []

    def process_source(src_records, ccy_key, custfiss_key, custno_val_fn, dealtype_key, dealref_key):
        for r in src_records:
            if not (r.get('item') and r.get('matdt')):
                continue

            matdt = r['matdt']
            issdt = r.get('issdt')

            # IF MATDT - REPTDATE < 8 THEN REMMTH = 0.1; ELSE %REMMTH;
            if (matdt - rep_date['date']).days < 8:
                remmth = 0.1
                rem30d = 0
            else:
                remmth, rem30d = calculate_remaining_months(
                    matdt, rep_date['date'], rep_date['days_in_month']
                )

            # IF MATDT - ISSDT < 8 THEN ORI30D = 0.1; ELSE ORI30D = (MATDT-ISSDT)/30;
            if issdt and (matdt - issdt).days < 8:
                ori30d = 0.1
            elif issdt:
                ori30d = (matdt - issdt).days / 30
            else:
                ori30d = 0

            part = r['part']
            item = r['item']
            bnmcode = f"{part}{item}00{format_mth_bucket(remmth)}0000Y"

            base = {
                'src': r['src'], 'bnmcode': bnmcode, 'part': part, 'item': item,
                'cur': r.get(ccy_key, 'MYR'), 'amt': r['amount'],
                'amtusd': r.get('amtusd', 0), 'amtsgd': r.get('amtsgd', 0),
                'custfiss': r.get(custfiss_key, 0), 'custno': custno_val_fn(r),
                'dealtype': r.get(dealtype_key, ''), 'dealref': r.get(dealref_key, ''),
                'remmth': remmth, 'rem30d': rem30d, 'ori30d': ori30d, 'matdt': matdt
            }
            all_records.append(base)

            # PART 1 duplicate: 95->93, else->94
            new_part = '93' if part == '95' else '94'
            dup = dict(base)
            dup['src'] = r['src'] + '_part1'
            dup['bnmcode'] = f"{new_part}{item}00{format_mth_bucket(remmth)}0000Y"
            dup['part'] = new_part
            all_records.append(dup)

    process_source(
        k1_records, ccy_key='gwccy', custfiss_key='gwc2r',
        custno_val_fn=lambda r: None,
        dealtype_key='gwdlp', dealref_key='gwdlr'
    )
    process_source(
        k3_records, ccy_key='utccy', custfiss_key='utctp',
        custno_val_fn=lambda r: r.get('utcus'),
        dealtype_key='utdlp', dealref_key='utdlr'
    )

    return all_records
