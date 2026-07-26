"""
EIMIR101 SAS to Python Conversion
Processes loan arrears reports with branch-level summaries

Output format matches the actual production listing exactly:
  - ASA carriage-control character in column 1 ('1' = new page,
    '0' = double-space-before, ' ' = single space / normal)
  - Fixed-column data rows built from the same @N column scheme as the
    original SAS PUT statement, shifted +1 to make room for the
    carriage-control character
  - 14 arrears buckets (<1, 1-2, 2-3, 3-4, 4-5, 5-6, 6-7, 7-8, 8-9,
    9-12, 12-18, 18-24, 24-36, >36 months) printed 5+5+4+0 per line,
    followed by SUBTOTAL >=3MTH, SUBTOTAL >=6MTH and TOTAL
"""

from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import pyreadstat
import numpy as np
from typing import Dict, List, Optional
import warnings
warnings.filterwarnings('ignore')

# Setup paths
BASE_PATH = Path(".")
INPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR101"
OUTPUT_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR101"
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# CCDTXT2 output path - appends to existing file
CCDTXT2_PATH = BASE_PATH / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMIR_CCDTXT2"
CCDTXT2_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================================
# 1. REPTDATE Processing (using yesterday's date)
# ============================================================================

def process_repdate() -> Dict[str, str]:
    """Process REPTDATE using yesterday's date"""
    repdate = date.today() - timedelta(days=1)

    return {
        'RDATE': repdate.strftime("%d%m%y"),        # DDMMYY8. (for filenames etc.)
        'RDATE_DISPLAY': repdate.strftime("%d/%m/%y"),  # DD/MM/YY (as printed on report)
        'REPTYEAR': str(repdate.year),              # YEAR4.
        'REPTMON': f"{repdate.month:02d}",          # Z2.
        'REPTDAY': f"{repdate.day:02d}",             # Z2.
        'REPTDATE': repdate
    }

# ============================================================================
# 2. Load and Process Loan Data
# ============================================================================

def load_loan_data() -> pd.DataFrame:
    """Load loan data from SAS file using pyreadstat"""
    loan_path = INPUT_PATH / "loantemp.sas7bdat"

    try:
        df, meta = pyreadstat.read_sas7bdat(loan_path)
        print(f"Loaded {len(df)} records from loantemp.sas7bdat")
        print(f"Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print(f"Error loading SAS file: {e}")
        return pd.DataFrame()

def load_branch_data() -> pd.DataFrame:
    """Load branch header data from fixed-width flat file"""
    branch_path = INPUT_PATH / "LKP_BRANCH"

    if branch_path.exists():
        try:
            with open(branch_path, 'r') as f:
                lines = f.readlines()

            data = []
            for line in lines:
                line = line.strip('\n').ljust(80)
                branch = line[0:3].strip()
                brhcode = line[3:10].strip()

                if branch and brhcode:
                    try:
                        data.append({
                            'BRANCH': int(branch),
                            'BRHCODE': brhcode
                        })
                    except ValueError:
                        continue

            df = pd.DataFrame(data)
            print(f"Loaded {len(df)} records from LKP_BRANCH")
            return df
        except Exception as e:
            print(f"Error loading branch file: {e}")
            return pd.DataFrame()
    else:
        print("Branch file not found, creating default branch mapping")
        return pd.DataFrame()

def create_default_branches(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Create default branch data from loan data if branch file doesn't exist"""
    if 'BRANCH' not in loan_df.columns:
        return pd.DataFrame()

    unique_branches = loan_df['BRANCH'].unique()
    branch_data = []
    for branch in unique_branches:
        if pd.notna(branch):
            branch_data.append({
                'BRANCH': int(branch),
                'BRHCODE': f"BR{int(branch):03d}"
            })

    df = pd.DataFrame(branch_data)
    print(f"Created {len(df)} default branch records")
    return df

# ============================================================================
# 3. Categorize Loans
# ============================================================================

def categorize_loans(loan_df: pd.DataFrame, hpd_list: List[str]) -> pd.DataFrame:
    """Categorize loans into different types (A,B,C,D)"""

    if loan_df.empty:
        return pd.DataFrame()

    filtered_df = loan_df[
        (loan_df.get('BALANCE', 0) > 0) &
        (loan_df.get('BORSTAT', '') != 'Z')
    ].copy()

    print(f"Filtered to {len(filtered_df)} records (BALANCE>0 and BORSTAT!='Z')")

    if filtered_df.empty:
        return pd.DataFrame()

    hpd_numbers = [int(x.strip("'")) for x in hpd_list]

    categorized_rows = []

    cat_a = filtered_df[filtered_df['PRODUCT'].isin([380, 381, 700, 705, 720, 725])].copy()
    if not cat_a.empty:
        cat_a['CAT'] = 'A'
        cat_a['TYPE'] = '(HPD-C)'
        categorized_rows.append(cat_a)
        print(f"  Category A: {len(cat_a)} records")

    cat_b = filtered_df[filtered_df['PRODUCT'].isin([380, 381])].copy()
    if not cat_b.empty:
        cat_b['CAT'] = 'B'
        cat_b['TYPE'] = '(HP 380/381)'
        categorized_rows.append(cat_b)
        print(f"  Category B: {len(cat_b)} records")

    cat_c = filtered_df[filtered_df['PRODUCT'].isin([128, 130, 131, 132])].copy()
    if not cat_c.empty:
        cat_c['CAT'] = 'C'
        cat_c['TYPE'] = '(AITAB)'
        categorized_rows.append(cat_c)
        print(f"  Category C: {len(cat_c)} records")

    cat_d = filtered_df[filtered_df['PRODUCT'].isin(hpd_numbers)].copy()
    if not cat_d.empty:
        cat_d['CAT'] = 'D'
        cat_d['TYPE'] = '(-HPD-)'
        categorized_rows.append(cat_d)
        print(f"  Category D: {len(cat_d)} records")

    if categorized_rows:
        combined = pd.concat(categorized_rows, ignore_index=True)
        print(f"Total categorized: {len(combined)} records")
        return combined
    else:
        return pd.DataFrame()

# ============================================================================
# 4. Merge with Branch Data
# ============================================================================

def merge_branch_data(loan_df: pd.DataFrame, branch_df: pd.DataFrame) -> pd.DataFrame:
    """Merge loan data with branch header data"""
    if loan_df.empty:
        return loan_df

    if branch_df.empty:
        branch_df = create_default_branches(loan_df)

    if branch_df.empty:
        print("No branch data available, proceeding without branch merge")
        loan_df['BRHCODE'] = loan_df['BRANCH'].apply(lambda x: f"BR{int(x):03d}" if pd.notna(x) else '')
        return loan_df

    merged = loan_df.merge(branch_df, on='BRANCH', how='left')

    if 'BRHCODE' in merged.columns:
        merged['BRHCODE'] = merged['BRHCODE'].fillna(
            merged['BRANCH'].apply(lambda x: f"BR{int(x):03d}" if pd.notna(x) else '')
        )
    else:
        merged['BRHCODE'] = merged['BRANCH'].apply(lambda x: f"BR{int(x):03d}" if pd.notna(x) else '')

    print(f"Merged records: {len(merged)}")
    return merged

# ============================================================================
# 5. Build Branch-Level Summary (CAT, BRANCH, BRHCODE, TYPE x 14 buckets)
# ============================================================================

N_BUCKETS = 14

def build_branch_summary(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to one row per CAT/BRANCH with NOACC1..14 and BRHAMT1..14,
    plus the SUBTOTAL >=3MTH / >=6MTH / TOTAL columns used by the report."""

    if loan_df.empty:
        return pd.DataFrame()

    if 'ARREAR2' not in loan_df.columns:
        if 'ARREAR' in loan_df.columns:
            loan_df = loan_df.copy()
            loan_df['ARREAR2'] = loan_df['ARREAR']
        else:
            print("No arrears column found, using default value 0")
            loan_df = loan_df.copy()
            loan_df['ARREAR2'] = 0

    filtered = loan_df[(loan_df['ARREAR2'] >= 1) & (loan_df['ARREAR2'] <= N_BUCKETS)].copy()

    if filtered.empty:
        print("No records in arrears buckets 1-14")
        return pd.DataFrame()

    print(f"Records in arrears buckets 1-14: {len(filtered)}")

    grouped = (
        filtered.groupby(['CAT', 'BRANCH', 'BRHCODE', 'TYPE', 'ARREAR2'])
        .agg(NOACC=('BALANCE', 'count'), BRHAMT=('BALANCE', 'sum'))
        .reset_index()
    )

    # pivot to wide: one row per CAT/BRANCH/BRHCODE/TYPE, columns NOACC1..14 / BRHAMT1..14
    idx_cols = ['CAT', 'BRANCH', 'BRHCODE', 'TYPE']
    noacc_wide = grouped.pivot_table(index=idx_cols, columns='ARREAR2', values='NOACC', fill_value=0)
    brhamt_wide = grouped.pivot_table(index=idx_cols, columns='ARREAR2', values='BRHAMT', fill_value=0)

    # ensure all buckets 1..14 exist
    for i in range(1, N_BUCKETS + 1):
        if i not in noacc_wide.columns:
            noacc_wide[i] = 0
        if i not in brhamt_wide.columns:
            brhamt_wide[i] = 0
    noacc_wide = noacc_wide[[i for i in range(1, N_BUCKETS + 1)]]
    brhamt_wide = brhamt_wide[[i for i in range(1, N_BUCKETS + 1)]]
    noacc_wide.columns = [f'NOACC{i}' for i in range(1, N_BUCKETS + 1)]
    brhamt_wide.columns = [f'BRHAMT{i}' for i in range(1, N_BUCKETS + 1)]

    result = pd.concat([noacc_wide, brhamt_wide], axis=1).reset_index()

    # subtotal / total columns (buckets are 1-indexed: 4-14 -> >=3MTH, 7-14 -> >=6MTH)
    noacc_cols_4_14 = [f'NOACC{i}' for i in range(4, N_BUCKETS + 1)]
    brhamt_cols_4_14 = [f'BRHAMT{i}' for i in range(4, N_BUCKETS + 1)]
    noacc_cols_7_14 = [f'NOACC{i}' for i in range(7, N_BUCKETS + 1)]
    brhamt_cols_7_14 = [f'BRHAMT{i}' for i in range(7, N_BUCKETS + 1)]
    noacc_cols_1_14 = [f'NOACC{i}' for i in range(1, N_BUCKETS + 1)]
    brhamt_cols_1_14 = [f'BRHAMT{i}' for i in range(1, N_BUCKETS + 1)]

    result['SUBACC'] = result[noacc_cols_4_14].sum(axis=1)
    result['SUBBRH'] = result[brhamt_cols_4_14].sum(axis=1)
    result['SUBAC2'] = result[noacc_cols_7_14].sum(axis=1)
    result['SUBBR2'] = result[brhamt_cols_7_14].sum(axis=1)
    result['SOTACC'] = result[noacc_cols_1_14].sum(axis=1)
    result['TOTBRH'] = result[brhamt_cols_1_14].sum(axis=1)

    result = result.sort_values(['CAT', 'BRANCH']).reset_index(drop=True)
    print(f"Created summary with {len(result)} branch records")
    return result

# ============================================================================
# 6. Report Filters (Report A = all categorized; Report B = filtered subset)
# ============================================================================

def generate_report_a(loan_data: pd.DataFrame) -> pd.DataFrame:
    """Report A: all categorized records"""
    return loan_data.copy()

def generate_report_b(loan_data: pd.DataFrame, hpd_list: List[str]) -> pd.DataFrame:
    """Report B: excludes AITAB/-HPD- types and borrower statuses F/I/R,
    restricted to the HPD product list"""
    if loan_data.empty:
        return pd.DataFrame()

    hpd_numbers = [int(x.strip("'")) for x in hpd_list]

    filtered = loan_data[
        (~loan_data['TYPE'].isin(['(AITAB)', '(-HPD-)'])) &
        (~loan_data['BORSTAT'].isin(['F', 'I', 'R'])) &
        (loan_data['PRODUCT'].isin(hpd_numbers))
    ].copy()

    print(f"Report B filtered records: {len(filtered)}")
    return filtered

# ============================================================================
# 7. Fixed-column report writer (matches production output byte-for-byte)
# ============================================================================

LINE_WIDTH = 133

# Column map for the 5 (NOACC,BRHAMT) group pairs within a print line.
# These are the same absolute @N positions as the underlying SAS PUT
# statement, shifted +1 because column 1 is reserved for the ASA
# carriage-control character in this report's print file.
GROUP_STARTS   = [6,  14, 31, 39, 55, 63, 79, 88, 106, 115]
GROUP_WIDTHS   = [7,  16,  7, 15,  7, 15,  8, 17,   8,  17]
GROUP_DECIMALS = [0,   2,  0,  2,  0,  2,  0,  2,   0,   2]

BRANCH_COL = 2  # BRANCH / BRHCODE / 'TOT' label column


def fmt_num(val, width, decimals):
    if decimals == 0:
        s = f"{int(round(val)):,}"
    else:
        s = f"{float(val):,.2f}"
    return s.rjust(width)


def build_line(control, lead_text, slot_values):
    """control: 1-char ASA carriage-control code ('1', '0', ' ', or '-')
    lead_text: text for the BRANCH/BRHCODE/'TOT' column (col 2), or None
    slot_values: dict {slot_index (0-9): numeric value} -> placed at
                 GROUP_STARTS[slot]/GROUP_WIDTHS[slot]/GROUP_DECIMALS[slot]
    """
    buf = [' '] * LINE_WIDTH
    buf[0] = control
    if lead_text:
        start = BRANCH_COL - 1
        buf[start:start + len(lead_text)] = list(lead_text)
    for slot, val in slot_values.items():
        col, width, dec = GROUP_STARTS[slot], GROUP_WIDTHS[slot], GROUP_DECIMALS[slot]
        text = fmt_num(val, width, dec)
        start = col - 1
        buf[start:start + width] = list(text)
    return ''.join(buf)


def text_line(control, segments, width=LINE_WIDTH):
    """Place literal text segments at fixed 1-indexed columns.
    segments: list of (col, text)"""
    buf = [' '] * width
    buf[0] = control
    for col, text in segments:
        start = col - 1
        end = start + len(text)
        if end > len(buf):
            buf.extend([' '] * (end - len(buf)))
        buf[start:end] = list(text)
    return ''.join(buf)


def dash_line(control=' '):
    return text_line(control, [(2, '-' * 130)])


def page_header(progid, page_num, type_label, rdate_display):
    lines = []
    lines.append(text_line('1', [
        (2, "PROGRAM-ID : EIMAR101" + progid),
        (44, "P U B L I C   I S L A M I C   B A N K   B E R H A D"),
        (119, f"PAGE NO.: {page_num}"),
    ]))
    lines.append(text_line(' ', [
        (46, "OUTSTANDING LOANS IN ARREARS "),
        (75, f"{type_label:<13}"),
        (89, rdate_display),
    ]))
    lines.append(text_line('0', [
        (2, "BRH    NO          < 1 MTH"), (34, "NO     1 TO < 2 MTH"),
        (59, "NO     2 TO < 3 MTH"), (85, "NO      3 TO < 4 MTH"),
        (112, "NO      4 TO < 5 MTH"),
    ]))
    lines.append(text_line(' ', [
        (2, "       NO     5 TO < 6 MTH"), (34, "NO     6 TO < 7 MTH"),
        (59, "NO     7 TO < 8 MTH"), (85, "NO      8 TO < 9 MTH"),
        (112, "NO     9 TO < 12 MTH"),
    ]))
    lines.append(text_line(' ', [
        (2, "       NO   12 TO < 18 MTH"), (34, "NO   18 TO < 24 MTH"),
        (59, "NO   24 TO < 36 MTH"), (85, "NO          > 36 MTH"),
        (112, "NO   SUBTOTAL >=3MTH"),
    ]))
    lines.append(text_line(' ', [
        (85, "NO   SUBTOTAL >=6MTH"), (112, "NO             TOTAL"),
    ]))
    lines.append(dash_line(' '))
    return lines


def write_branch_block(branch_row):
    """Return the 4 print lines for one branch."""
    BRANCH = str(int(branch_row['BRANCH'])).zfill(3)
    BRHCODE = str(branch_row['BRHCODE'] or '').strip()

    noacc = [branch_row[f'NOACC{i}'] for i in range(1, N_BUCKETS + 1)]
    brhamt = [branch_row[f'BRHAMT{i}'] for i in range(1, N_BUCKETS + 1)]
    subacc, subbrh = branch_row['SUBACC'], branch_row['SUBBRH']
    subac2, subbr2 = branch_row['SUBAC2'], branch_row['SUBBR2']
    sotacc, totbrh = branch_row['SOTACC'], branch_row['TOTBRH']

    l1 = build_line(' ', BRANCH, {
        0: noacc[0], 1: brhamt[0], 2: noacc[1], 3: brhamt[1], 4: noacc[2], 5: brhamt[2],
        6: noacc[3], 7: brhamt[3], 8: noacc[4], 9: brhamt[4],
    })
    l2 = build_line(' ', BRHCODE, {
        0: noacc[5], 1: brhamt[5], 2: noacc[6], 3: brhamt[6], 4: noacc[7], 5: brhamt[7],
        6: noacc[8], 7: brhamt[8], 8: noacc[9], 9: brhamt[9],
    })
    l3 = build_line(' ', None, {
        0: noacc[10], 1: brhamt[10], 2: noacc[11], 3: brhamt[11], 4: noacc[12], 5: brhamt[12],
        6: noacc[13], 7: brhamt[13], 8: subacc, 9: subbrh,
    })
    l4 = build_line(' ', None, {
        6: subac2, 7: subbr2, 8: sotacc, 9: totbrh,
    })
    return [l1, l2, l3, l4]


def write_totals_block(totals):
    """totals: dict with keys NOACC1..14, BRHAMT1..14, SUBACC, SUBBRH, SUBAC2, SUBBR2, SOTACC, TOTBRH"""
    noacc = [totals[f'NOACC{i}'] for i in range(1, N_BUCKETS + 1)]
    brhamt = [totals[f'BRHAMT{i}'] for i in range(1, N_BUCKETS + 1)]

    l1 = build_line(' ', "TOT", {
        0: noacc[0], 1: brhamt[0], 2: noacc[1], 3: brhamt[1], 4: noacc[2], 5: brhamt[2],
        6: noacc[3], 7: brhamt[3], 8: noacc[4], 9: brhamt[4],
    })
    l2 = build_line(' ', None, {
        0: noacc[5], 1: brhamt[5], 2: noacc[6], 3: brhamt[6], 4: noacc[7], 5: brhamt[7],
        6: noacc[8], 7: brhamt[8], 8: noacc[9], 9: brhamt[9],
    })
    l3 = build_line(' ', None, {
        0: noacc[10], 1: brhamt[10], 2: noacc[11], 3: brhamt[11], 4: noacc[12], 5: brhamt[12],
        6: noacc[13], 7: brhamt[13], 8: totals['SUBACC'], 9: totals['SUBBRH'],
    })
    l4 = build_line(' ', None, {
        6: totals['SUBAC2'], 7: totals['SUBBR2'], 8: totals['SOTACC'], 9: totals['TOTBRH'],
    })
    return [l1, l2, l3, l4]


def write_fixed_width_report(branch_summary: pd.DataFrame, progid: str, variables: Dict, filename: str):
    """Write the report exactly as produced in production (progid e.g. '-A' or '-B')."""
    if branch_summary.empty:
        print(f"No data for report {progid}")
        return

    txt_path = OUTPUT_PATH / filename
    categories = sorted(branch_summary['CAT'].unique())

    with open(txt_path, 'w') as f:
        page_num = 0
        for cat in categories:
            page_num += 1
            cat_data = branch_summary[branch_summary['CAT'] == cat].sort_values('BRANCH')
            type_label = cat_data['TYPE'].iloc[0]

            for line in page_header(progid, page_num, type_label, variables['RDATE_DISPLAY']):
                f.write(line + "\n")

            value_cols = [f'NOACC{i}' for i in range(1, N_BUCKETS + 1)] + \
                         [f'BRHAMT{i}' for i in range(1, N_BUCKETS + 1)] + \
                         ['SUBACC', 'SUBBRH', 'SUBAC2', 'SUBBR2', 'SOTACC', 'TOTBRH']
            totals = {c: 0 for c in value_cols}

            for _, branch_row in cat_data.iterrows():
                for line in write_branch_block(branch_row):
                    f.write(line + "\n")
                for c in value_cols:
                    totals[c] += branch_row[c]

            f.write(dash_line(' ') + "\n")
            for line in write_totals_block(totals):
                f.write(line + "\n")
            f.write(dash_line(' ') + "\n")

    print(f"✓ Report saved to: {txt_path}")


def append_to_ccdtxt2(report_content: str, variables: Dict):
    """Append report content to CCDTXT2.txt file"""
    ccdtxt2_file = CCDTXT2_PATH / "CCDTXT2.txt"
    
    # Create directory if it doesn't exist
    ccdtxt2_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Append content with date header
    with open(ccdtxt2_file, 'a') as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"Report Date: {variables['RDATE_DISPLAY']}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*80}\n\n")
        f.write(report_content)
        f.write("\n")
    
    print(f"✓ Appended to CCDTXT2.txt at: {ccdtxt2_file}")


def write_ccdtxt2_report(branch_summary: pd.DataFrame, progid: str, variables: Dict):
    """Generate report content and append to CCDTXT2.txt"""
    if branch_summary.empty:
        print(f"No data to append for CCDTXT2")
        return

    # Generate report content as string
    lines = []
    categories = sorted(branch_summary['CAT'].unique())
    
    for cat in categories:
        cat_data = branch_summary[branch_summary['CAT'] == cat].sort_values('BRANCH')
        type_label = cat_data['TYPE'].iloc[0]
        
        for line in page_header(progid, 1, type_label, variables['RDATE_DISPLAY']):
            lines.append(line)
        
        value_cols = [f'NOACC{i}' for i in range(1, N_BUCKETS + 1)] + \
                     [f'BRHAMT{i}' for i in range(1, N_BUCKETS + 1)] + \
                     ['SUBACC', 'SUBBRH', 'SUBAC2', 'SUBBR2', 'SOTACC', 'TOTBRH']
        totals = {c: 0 for c in value_cols}
        
        for _, branch_row in cat_data.iterrows():
            for line in write_branch_block(branch_row):
                lines.append(line)
            for c in value_cols:
                totals[c] += branch_row[c]
        
        lines.append(dash_line(' '))
        for line in write_totals_block(totals):
            lines.append(line)
        lines.append(dash_line(' '))
    
    # Join all lines with newline
    report_content = "\n".join(lines)
    
    # Append to CCDTXT2.txt
    append_to_ccdtxt2(report_content, variables)

# ============================================================================
# 8. Main Execution
# ============================================================================

def main():
    """Main execution function"""
    print("=" * 60)
    print("EIMIR101 SAS to Python Conversion")
    print("=" * 60)

    HPD_LIST = ["110", "115", "700", "705", "720", "725"]

    print("\n1. Processing REPTDATE (yesterday)...")
    variables = process_repdate()
    print(f"   Report Date: {variables['RDATE']} ({variables['RDATE_DISPLAY']})")

    print("\n2. Loading data...")
    loan_df = load_loan_data()
    branch_df = load_branch_data()
    print(f"   Loans: {len(loan_df)}, Branches: {len(branch_df)}")

    if loan_df.empty:
        print("ERROR: No loan data loaded")
        return

    print("\n3. Categorizing loans...")
    categorized = categorize_loans(loan_df, HPD_LIST)

    if categorized.empty:
        print("ERROR: No categorized records")
        return

    print("\n4. Merging with branch data...")
    merged_data = merge_branch_data(categorized, branch_df)

    print("\n5. Generating Report A (EIMAR101-A)...")
    report_a = generate_report_a(merged_data)
    summary_a = build_branch_summary(report_a)
    print(f"   Report A: {len(report_a)} records, {len(summary_a)} branch summaries")

    print("\n6. Generating Report B (EIMAR101-B)...")
    report_b = generate_report_b(merged_data, HPD_LIST)
    summary_b = build_branch_summary(report_b)
    print(f"   Report B: {len(report_b)} records, {len(summary_b)} branch summaries")

    print("\n7. Writing fixed-column production-format reports...")
    if not summary_a.empty:
        write_fixed_width_report(summary_a, "-A", variables, f"EIMAR101-A_{variables['RDATE']}.txt")
        # Append Report A to CCDTXT2
        write_ccdtxt2_report(summary_a, "-A", variables)
    
    if not summary_b.empty:
        write_fixed_width_report(summary_b, "-B", variables, f"EIMAR101-B_{variables['RDATE']}.txt")
        # Append Report B to CCDTXT2
        write_ccdtxt2_report(summary_b, "-B", variables)

    print("\n8. Saving parquet files for reference...")
    if not merged_data.empty:
        merged_data.to_parquet(OUTPUT_PATH / "LOANTEMP_CATEGORIZED.parquet")
    if not summary_a.empty:
        summary_a.to_parquet(OUTPUT_PATH / "REPORT_A_SUMMARY.parquet")
    if not summary_b.empty:
        summary_b.to_parquet(OUTPUT_PATH / "REPORT_B_SUMMARY.parquet")

    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"Report A records: {len(report_a)}")
    print(f"Report B records: {len(report_b)}")
    print(f"Output saved to: {OUTPUT_PATH}")
    print(f"CCDTXT2 appended to: {CCDTXT2_PATH / 'CCDTXT2.txt'}")
    print("\nText Report Files:")
    for file in sorted(OUTPUT_PATH.glob("*.txt")):
        print(f"  - {file.name}")

if __name__ == "__main__":
    from datetime import datetime
    main()
