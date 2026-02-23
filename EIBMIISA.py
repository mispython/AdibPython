"""
EIBMIISA - Monthly Movement of Interest-In-Suspense (IIS) Report
Comprehensive IIS reporting for NPL accounts including:
- IIS opening/closing balances
- IIS recognized, recovered, written-off
- Realisable value calculation for collateral
- Multiple report formats (detail by NPL type, by branch)
"""

import polars as pl
from datetime import datetime, date, timedelta
from pathlib import Path
import numpy as np

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LOAN': 'data/loan/',
    'NPL': 'data/npl/',
    'PRVLOAN': 'data/prvloan/',
    'SASDATA': 'data/sasdata/',
    'ISASDATA': 'data/isasdata/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# Risk classification format ($RISKFMT)
RISK_FMT = {'1': 'SS1', '2': 'SS2', '3': 'D  ', '4': 'B  '}

# =============================================================================
# DATE PROCESSING
# =============================================================================
def get_report_dates():
    """Read datefile and set all macro variables"""
    datefile_path = Path(PATHS['OUTPUT']) / 'DATEFILE.txt'
    
    # Create sample datefile if not exists
    if not datefile_path.exists():
        with open(datefile_path, 'w') as f:
            f.write(f"{datetime.now().strftime('%m%d%Y'):<11}")
    
    with open(datefile_path, 'r') as f:
        line = f.readline().strip()
        extdate = int(line[:11])
    
    # Convert external date to date (MMDDYYYY format in Z11.)
    extdate_str = str(extdate).zfill(11)
    reptdate = datetime.strptime(extdate_str[:8], "%m%d%Y").date()
    
    # Previous month
    prev_month = reptdate.month - 1
    if prev_month == 0:
        prev_month = 12
        prev_year = reptdate.year - 1
    else:
        prev_year = reptdate.year
    
    now = datetime.now()
    
    return {
        'reptdate': reptdate,
        'prev_month': f"{prev_month:02d}",
        'prev_year': prev_year,
        'reptmon': f"{reptdate.month:02d}",
        'prevmON': f"{prev_month:02d}",
        'rdate_word': reptdate.strftime("%d %B %Y"),  # WORDDATX18.
        'rdate_dmy': reptdate.strftime("%d/%m/%Y"),   # DDMMYY10.
        'nowk': '4',  # Fixed as per SAS
        'rptdte': now.strftime("%Y%m%d"),
        'rpttime': now.strftime("%H%M%S"),
        'curday': f"{now.day:02d}",
        'curmth': f"{now.month:02d}",
        'curyr': str(now.year),
        'rptyr': str(reptdate.year),
        'rptdy': f"{reptdate.day:02d}"
    }

# =============================================================================
# DATA LOADING
# =============================================================================
def load_sasdata(rep_vars):
    """Load and combine conventional and Islamic loan data"""
    try:
        conv = pl.read_parquet(f"{PATHS['SASDATA']}LOAN{rep_vars['reptmon']}{rep_vars['nowk']}.parquet")
    except:
        conv = pl.DataFrame()
    
    try:
        islam = pl.read_parquet(f"{PATHS['ISASDATA']}LOAN{rep_vars['reptmon']}{rep_vars['nowk']}.parquet")
    except:
        islam = pl.DataFrame()
    
    if conv.is_empty() and islam.is_empty():
        return pl.DataFrame()
    
    combined = pl.concat([conv, islam]) if not conv.is_empty() and not islam.is_empty() \
               else (conv if not conv.is_empty() else islam)
    
    # Keep only ACCTNO, NOTENO, BALANCE and deduplicate
    combined = combined.select(['ACCTNO', 'NOTENO', 'BALANCE']).unique(subset=['ACCTNO', 'NOTENO'])
    
    return combined

def load_prev_npl(rep_vars):
    """Load previous month's NPL data"""
    try:
        prev = pl.read_parquet(f"{PATHS['NPL']}TOTIIS{rep_vars['prevmON']}.parquet")
        prev = prev.rename({'IISCLOSE': 'PREVIIS', 'OICLOSE': 'PREVOI'})
        prev = prev.select(['ACCTNO', 'NOTENO', 'PREVIIS', 'PREVOI']).unique()
        return prev
    except:
        return pl.DataFrame()

def load_lnnote():
    """Load loan note data (conventional and Islamic)"""
    conv = pl.DataFrame()
    islam = pl.DataFrame()
    
    try:
        conv = pl.read_parquet(f"{PATHS['LOAN']}LNNOTE.parquet")
    except:
        pass
    
    try:
        islam = pl.read_parquet(f"{PATHS['LOAN']}ILOAN.LNNOTE.parquet")
    except:
        pass
    
    if conv.is_empty() and islam.is_empty():
        return pl.DataFrame()
    
    combined = pl.concat([conv, islam]) if not conv.is_empty() and not islam.is_empty() \
               else (conv if not conv.is_empty() else islam)
    
    # Filter and deduplicate
    combined = combined.filter(
        (pl.col('REVERSED') != 'Y') & 
        pl.col('NOTENO').is_not_null() & 
        pl.col('NTBRCH').is_not_null()
    ).unique(subset=['ACCTNO', 'NOTENO'])
    
    return combined

def load_prev_loan():
    """Load previous loan data"""
    conv = pl.DataFrame()
    islam = pl.DataFrame()
    
    try:
        conv = pl.read_parquet(f"{PATHS['PRVLOAN']}LNNOTE.parquet")
    except:
        pass
    
    try:
        islam = pl.read_parquet(f"{PATHS['PRVLOAN']}IPRVLOAN.LNNOTE.parquet")
    except:
        pass
    
    if conv.is_empty() and islam.is_empty():
        return pl.DataFrame()
    
    combined = pl.concat([conv, islam]) if not conv.is_empty() and not islam.is_empty() \
               else (conv if not conv.is_empty() else islam)
    
    # Filter and rename
    combined = combined.filter(
        (pl.col('REVERSED') != 'Y') & 
        pl.col('NOTENO').is_not_null() & 
        pl.col('NTBRCH').is_not_null()
    ).select(['ACCTNO', 'NOTENO', 'CURBAL', 'REBATE']).unique(subset=['ACCTNO', 'NOTENO'])
    
    combined = combined.rename({'CURBAL': 'PREVBAL', 'REBATE': 'PRVREB'})
    
    return combined

def load_iisclose(rep_vars):
    """Load IIS close data"""
    try:
        iisclose = pl.read_parquet(f"{PATHS['NPL']}IISCLOSE{rep_vars['reptmon']}{rep_vars['nowk']}.parquet")
        if 'BRANCH' in iisclose.columns:
            iisclose = iisclose.drop('BRANCH')
        return iisclose
    except:
        return pl.DataFrame()

def load_iis(rep_vars):
    """Load IIS data"""
    try:
        iis = pl.read_parquet(f"{PATHS['NPL']}IIS{rep_vars['reptmon']}.parquet")
        return iis
    except:
        return pl.DataFrame()

def load_lncomm():
    """Load loan commitment data"""
    conv = pl.DataFrame()
    islam = pl.DataFrame()
    
    try:
        conv = pl.read_parquet(f"{PATHS['LOAN']}LNCOMM.parquet")
    except:
        pass
    
    try:
        islam = pl.read_parquet(f"{PATHS['LOAN']}ILOAN.LNCOMM.parquet")
    except:
        pass
    
    if conv.is_empty() and islam.is_empty():
        return pl.DataFrame()
    
    combined = pl.concat([conv, islam]) if not conv.is_empty() and not islam.is_empty() \
               else (conv if not conv.is_empty() else islam)
    
    combined = combined.select(['ACCTNO', 'COMMNO', 'CCURAMT', 'CUSEDAMT']).unique(subset=['ACCTNO', 'COMMNO'])
    
    return combined

# =============================================================================
# IIS CALCULATION ENGINE
# =============================================================================
def calculate_iis(row, rep_date):
    """
    Core IIS calculation logic (matches SAS logic exactly)
    Handles both Accrual (NTINT='A') and Non-Accrual cases
    """
    result = row.copy()
    
    # Initialize fields
    result['CLASSOLD'] = ' '
    result['CLASSNEW'] = ' '
    result['AGEING'] = 0
    result['DIFFBAL'] = 0
    result['IISP'] = 0
    result['OIP'] = 0
    result['IIS'] = 0
    result['IISR'] = 0
    result['OI'] = 0
    result['OIREC'] = 0
    result['IISCLOSE'] = 0
    result['OICLOSE'] = 0
    result['TOTOPEN'] = 0
    result['TOTIIS'] = 0
    result['TOTIISR'] = 0
    result['TOTWOF'] = 0
    result['TOTCLOSE'] = 0
    
    # Ageing calculation
    if row.get('BLDATE', 0) > 0:
        try:
            bldate = datetime.strptime(str(int(row['BLDATE'])).zfill(8), "%m%d%Y").date()
            result['AGEING'] = (rep_date - bldate).days
        except:
            pass
    
    # Special handling for certain note ranges
    if row.get('OLDNOTEDAYARR', 0) > 0 and 98000 <= row.get('NOTENO', 0) <= 98999:
        if result['AGEING'] < 0:
            result['AGEING'] = 0
        result['AGEING'] += row['OLDNOTEDAYARR']
    
    # Risk classification
    riskrate = row.get('RISKRATE', '')
    if riskrate and riskrate != ' ':
        result['CLASSNEW'] = RISK_FMT.get(str(riskrate).strip(), ' ')
    
    # Branch formatting
    ntbrch = row.get('NTBRCH', 0)
    result['BRANCH'] = f"{int(ntbrch):03d} {ntbrch}"  # Simple format, would need BRCHCD format
    
    # Difference balance
    prevbal = row.get('PREVBAL', 0) or 0
    curbal = row.get('CURBAL', 0) or 0
    spwof = row.get('SPWOF', 0) or 0
    iisw = row.get('IISW', 0) or 0
    adjust = row.get('ADJUST', 0) or 0
    result['DIFFBAL'] = prevbal - curbal - spwof - iisw - adjust
    
    # Previous IIS/OI
    result['IISP'] = row.get('PREVIIS', 0) or 0
    result['OIP'] = row.get('PREVOI', 0) or 0
    
    nplind = row.get('NPLIND', '')
    loantype = row.get('LOANTYPE', 0)
    lsttrncd = row.get('LSTTRNCD', 0)
    iisr = row.get('IISR', 0) or 0
    iisw = row.get('IISW', 0) or 0
    ntint = row.get('NTINT', '')
    
    # ACCRUAL CASE (NTINT = 'A')
    if ntint == 'A':
        # Initial IIS calculation for O/P NPL
        if nplind in ('O', 'P'):
            result['IIS'] = (iisr + iisw) - result['IISP']
        
        # Special LSTTRNAM handling
        if loantype in (517, 521, 522, 523, 528) and nplind == 'N' and lsttrncd == 166:
            result['LSTTRNAM'] = row.get('LSTTRNAM', 0) / 100000
        else:
            result['LSTTRNAM'] = 0
        
        iiscls = row.get('IISCLS', 0) or 0
        oi = 0
        oirec = 0
        
        # IIS/OI calculations
        if iiscls != 0:
            result['IISP'] = iiscls - result['OIP']
        
        if nplind in ('O', 'P'):
            result['IIS'] = iisr + iisw - result['IISP']
        
        result['IIS'] = row.get('ADONIIS', 0) or 0
        oi = (row.get('FEEASSES', 0) or 0) - (row.get('FEEWAIVE', 0) or 0)
        if oi < 0:
            oi = 0
        oirec = row.get('FEEPAY', 0) or 0
        
        borstat = row.get('BORSTAT', '')
        if borstat in ('W', 'X') and nplind != 'P':
            result['IIS'] = 0
            oi = row.get('FEELWTOT', 0) or 0
            # oirec stays as oip (commented in SAS)
        
        if nplind == 'E' and borstat == 'P' or lsttrncd == 652:
            iisr = 0
        elif row.get('LOANSTAT', 0) == 1 or result['DIFFBAL'] >= result['IISP']:
            iisr = result['IISP'] - iisw
        elif result['DIFFBAL'] < result['IISP']:
            iisr = result['DIFFBAL'] - iisw
        
        if nplind == 'O':
            # iisr = result['IISP'] - iisw (commented)
            result['IIS'] = 0
            oirec = (row.get('FEEPAY', 0) or 0) + (row.get('FEELWTOT', 0) or 0)
        
        if iisr < 0:
            iisr = 0
        result['IISR'] = iisr
        
        if nplind == 'P':
            # iisr = iisr + result['IIS'] (commented)
            result['IIS'] = result['IIS'] + iisr + iisw - result['IISP']
        
        result['IISCLOSE'] = result['IISP'] + result['IIS'] - iisr - iisw
        if result['IISCLOSE'] < 0:
            result['IISCLOSE'] = 0
        
        if nplind == 'O':
            result['OICLOSE'] = 0
        else:
            result['OICLOSE'] = row.get('FEELWTOT', 0) or 0
        
        result['TOTOPEN'] = result['IISP'] + result['OIP']
        result['TOTIIS'] = result['IIS'] + oi
        result['TOTIISR'] = iisr + oirec
        result['TOTWOF'] = iisw
        
        if nplind == 'N':
            result['TOTIIS'] = result['IISCLOSE'] + result['TOTIISR'] + result['TOTWOF']
            result['TOTOPEN'] = 0
        
        result['TOTCLOSE'] = result['IISCLOSE'] + result['OICLOSE']
        
        if borstat in ('W', 'X') and nplind != 'P':
            result['TOTCLOSE'] = result['TOTOPEN'] - result['TOTIISR'] - result['TOTWOF']
    
    # NON-ACCRUAL CASE
    else:
        if nplind == 'O':
            actiaccr = row.get('ACTIACCR', 0) or 0
            if actiaccr == 0:
                intactiv = row.get('INTACTIV', 0) or 0
                if intactiv < 0:
                    iisr = iisr + (row.get('FEELWTOT', 0) or 0) - intactiv
                else:
                    iisr = iisr + (row.get('FEELWTOT', 0) or 0)
                result['IIS'] = iisr + iisw - result['IISP']
            else:
                result['IIS'] = iisr + iisw + actiaccr - result['IISP']
                result['OIREC'] = (row.get('FEEPAY', 0) or 0) + (row.get('FEELWTOT', 0) or 0)
        
        result['TOTOPEN'] = result['IISP']
        result['TOTIISR'] = iisr
        result['TOTWOF'] = iisw
        result['TOTIIS'] = result['IIS']
        
        if nplind == 'N':
            iisclose = row.get('IISCLOSE', 0) or 0
            result['TOTIIS'] = iisclose + result['TOTIISR'] + result['TOTWOF']
            result['TOTOPEN'] = 0
        
        if nplind == 'O':
            result['TOTCLOSE'] = result['IISP'] + result['IIS'] - iisr - iisw
        else:
            result['TOTCLOSE'] = row.get('IISCLOSE', 0) or 0
    
    return result

# =============================================================================
# REALISABLE VALUE CALCULATION (%SPRO MACRO)
# =============================================================================
def calculate_realisable_value(row, rep_date):
    """
    %SPRO macro - Calculate realisable value for collateral
    Handles different liability codes and flag types (F, P, I)
    """
    result = row.copy()
    
    # Calculate last update date
    lastupdt = None
    dlivrydt = row.get('DLIVRYDT', 0)
    apprdate = row.get('APPRDATE', 0)
    issuedt = row.get('ISSUEDT', 0)
    
    if dlivrydt and dlivrydt != 0:
        dlivry_str = str(int(dlivrydt)).zfill(11)[:8]
        lastupdt = datetime.strptime(dlivry_str, "%m%d%Y").date()
    elif apprdate and apprdate != 0:
        appr_str = str(int(apprdate)).zfill(11)[:8]
        lastupdt = datetime.strptime(appr_str, "%m%d%Y").date()
    else:
        iss_str = str(int(issuedt)).zfill(11)[:8]
        lastupdt = datetime.strptime(iss_str, "%m%d%Y").date()
    
    result['LASTUPDT'] = lastupdt
    
    # Calculate days since last update
    days_since = (rep_date - lastupdt).days if lastupdt else 0
    
    # Initialize realisvl
    realisvl = 0
    flag1 = row.get('FLAG1', '')
    liabcode = row.get('LIABCODE', '')
    marketvl = row.get('MARKETVL', 0) or 0
    appvalue = row.get('APPVALUE', 0) or 0
    loantype = row.get('LOANTYPE', 0)
    
    # FLAG1 = 'F' (Fully secured)
    if flag1 == 'F':
        # First category: LIABCODE in ('50','C2')
        if liabcode in ('50', 'C2'):
            if marketvl > 0 and appvalue > 0:
                if marketvl == appvalue:
                    realisvl = marketvl
                    if days_since >= 730:
                        if liabcode == '50':
                            realisvl *= 0.9  # 10% reduction
                        else:
                            realisvl *= 0.8  # 20% reduction
                else:
                    if dlivrydt and dlivrydt > 0:
                        realisvl = marketvl
                    else:
                        realisvl = min(marketvl, appvalue) if marketvl < appvalue else appvalue
                    if days_since >= 730:
                        if liabcode == '50':
                            realisvl *= 0.9
                        else:
                            realisvl *= 0.8
            
            elif marketvl > 0 and appvalue == 0:
                realisvl = marketvl
                if days_since >= 730:
                    if liabcode == '50':
                        realisvl *= 0.9
                    else:
                        realisvl *= 0.8
            
            elif marketvl == 0 and appvalue > 0:
                realisvl = appvalue
                if days_since >= 730:
                    if liabcode == '50':
                        realisvl *= 0.9
                    else:
                        realisvl *= 0.8
        
        # Second category: Other liability codes
        elif liabcode in ('B8', 'B9', '33', '37', '34', '32', 'C1'):
            if marketvl > 0 and appvalue > 0:
                if marketvl == appvalue:
                    realisvl = marketvl
                    if days_since >= 730:
                        realisvl *= 0.75  # 25% reduction
                else:
                    if dlivrydt and dlivrydt > 0:
                        realisvl = marketvl
                    else:
                        realisvl = min(marketvl, appvalue) if marketvl < appvalue else appvalue
                    if days_since >= 730:
                        realisvl *= 0.75
            
            elif marketvl > 0 and appvalue == 0:
                realisvl = marketvl
                if days_since >= 730:
                    realisvl *= 0.75
            
            elif marketvl == 0 and appvalue > 0:
                realisvl = appvalue
                if days_since >= 730:
                    realisvl *= 0.75
        
        # Default case
        else:
            realisvl = marketvl
            if loantype in (300, 301) and days_since >= 730:
                realisvl *= 0.9  # 10% reduction
    
    # FLAG1 in ('P', 'I') (Partially secured / Insufficient)
    elif flag1 in ('P', 'I'):
        cusedamt = row.get('CUSEDAMT', 0) or 0
        cc uramt = row.get('CCURAMT', 0) or 0
        undrawn = cc uramt - cusedamt
        result['UNDRAWN'] = undrawn
        
        # LIABCODE in ('50','C2') with undrawn
        if liabcode in ('50', 'C2'):
            if undrawn > 0:
                apprdt = None
                if apprdate and apprdate != 0:
                    appr_str = str(int(apprdate)).zfill(11)[:8]
                    apprdt = datetime.strptime(appr_str, "%m%d%Y").date()
                
                days_since_appr = (rep_date - apprdt).days if apprdt else 0
                
                if days_since_appr <= 730:
                    realisvl = cusedamt  # Based on comment SMR A503
                else:
                    if liabcode == '50':
                        realisvl = cusedamt * 0.9
                    else:
                        realisvl = cusedamt * 0.8
            else:
                if marketvl > 0:
                    realisvl = marketvl
                    if days_since >= 730:
                        if liabcode == '50':
                            realisvl *= 0.9
                        else:
                            realisvl *= 0.8
                else:
                    realisvl = appvalue
                    if days_since >= 730:
                        if liabcode == '50':
                            realisvl *= 0.9
                        else:
                            realisvl *= 0.8
        
        # Default case
        else:
            realisvl = marketvl
            if loantype in (300, 301) and days_since >= 730:
                realisvl *= 0.9
    
    # Special overrides
    if loantype == 521:
        realisvl = row.get('CURRBAL', 0) or 0
    
    if row.get('BORSTAT', '') == 'F':
        realisvl = 0
    
    result['REALISVL'] = realisvl
    return result

# =============================================================================
# REPORT GENERATION
# =============================================================================
def generate_iis_report(df, title, footnote, output_file, 
                        filter_loantype=None, exclude_loantype=None,
                        group_by=None, page_by=None):
    """Generate IIS report in the format of PROC REPORT"""
    
    # Apply filters
    if filter_loantype:
        df = df.filter(pl.col('LOANTYPE').is_in(filter_loantype))
    if exclude_loantype:
        df = df.filter(~pl.col('LOANTYPE').is_in(exclude_loantype))
    
    if df.is_empty():
        return []
    
    lines = []
    lines.append("=" * 135)
    lines.append(title)
    lines.append("=" * 135)
    lines.append("")
    
    if group_by:
        # Sort by group
        df = df.sort(group_by)
        
        current_group = None
        group_total = {col: 0 for col in ['BALANCE', 'TOTOPEN', 'TOTIIS', 
                                           'TOTIISR', 'TOTWOF', 'TOTCLOSE']}
        grand_total = group_total.copy()
        
        for row in df.rows(named=True):
            group_val = row[group_by]
            
            # New group
            if group_val != current_group:
                if current_group is not None:
                    # Print group total
                    lines.append("-" * 135)
                    lines.append(f"SUBTOTAL FOR {group_by}: {current_group}")
                    lines.append(f"  Balance: {group_total['BALANCE']:>15,.2f}  "
                               f"TOTOPEN: {group_total['TOTOPEN']:>15,.2f}  "
                               f"TOTIIS: {group_total['TOTIIS']:>15,.2f}  "
                               f"TOTIISR: {group_total['TOTIISR']:>15,.2f}  "
                               f"TOTWOF: {group_total['TOTWOF']:>15,.2f}  "
                               f"TOTCLOSE: {group_total['TOTCLOSE']:>15,.2f}")
                    lines.append("-" * 135)
                    lines.append("")
                    
                    # Reset group total
                    group_total = {col: 0 for col in group_total}
                
                current_group = group_val
            
            # Print detail line
            lines.append(f"{row.get('BRANCH',''):<10} {row['ACCTNO']:>12} {row['NOTENO']:>8} "
                        f"{str(row.get('NAME',''))[:30]:<30} {row['LOANTYPE']:>5} "
                        f"{row.get('AGEING',0):>10} {row.get('CLASSNEW',''):<5} "
                        f"{row['BALANCE']:>15,.2f} {row.get('NPLIND',''):<8} "
                        f"{row.get('TOTOPEN',0):>15,.2f} {row.get('TOTIIS',0):>15,.2f} "
                        f"{row.get('TOTIISR',0):>15,.2f} {row.get('TOTWOF',0):>15,.2f} "
                        f"{row.get('TOTCLOSE',0):>15,.2f}")
            
            # Update totals
            for col in group_total:
                if col in row:
                    group_total[col] += row.get(col, 0)
                    grand_total[col] += row.get(col, 0)
    
    # Print grand total
    lines.append("=" * 135)
    lines.append("GRAND TOTAL")
    lines.append(f"  Balance: {grand_total['BALANCE']:>15,.2f}  "
                f"TOTOPEN: {grand_total['TOTOPEN']:>15,.2f}  "
                f"TOTIIS: {grand_total['TOTIIS']:>15,.2f}  "
                f"TOTIISR: {grand_total['TOTIISR']:>15,.2f}  "
                f"TOTWOF: {grand_total['TOTWOF']:>15,.2f}  "
                f"TOTCLOSE: {grand_total['TOTCLOSE']:>15,.2f}")
    lines.append("=" * 135)
    lines.append("")
    lines.append(f"Footnote: {footnote}")
    
    # Write to file
    with open(output_file, 'w') as f:
        for line in lines:
            f.write(f"{line}\n")
    
    return lines

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMIISA - Monthly Movement of Interest-In-Suspense")
    print("=" * 60)
    
    # Get report dates
    rep_vars = get_report_dates()
    print(f"\nReport Date: {rep_vars['rdate_word']}")
    print(f"Month: {rep_vars['reptmon']}, Previous: {rep_vars['prevmON']}")
    
    # Load all data sources
    print("\nLoading data sources...")
    
    sasdata = load_sasdata(rep_vars)
    print(f"  SASDATA: {len(sasdata)} records")
    
    prev_npl = load_prev_npl(rep_vars)
    print(f"  Previous NPL: {len(prev_npl)} records")
    
    lnnote = load_lnnote()
    print(f"  LNNOTE: {len(lnnote)} records")
    
    prev_loan = load_prev_loan()
    print(f"  Previous Loan: {len(prev_loan)} records")
    
    iisclose = load_iisclose(rep_vars)
    print(f"  IISCLOSE: {len(iisclose)} records")
    
    iis = load_iis(rep_vars)
    print(f"  IIS: {len(iis)} records")
    
    # Merge LNNOTE with previous loan and sasdata
    print("\nMerging loan data...")
    
    # Start with LNNOTE as base
    if lnnote.is_empty():
        print("  ERROR: No LNNOTE data found")
        return
    
    lnnote_merged = lnnote
    
    # Merge with previous loan
    if not prev_loan.is_empty():
        lnnote_merged = lnnote_merged.join(prev_loan, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Merge with sasdata
    if not sasdata.is_empty():
        lnnote_merged = lnnote_merged.join(sasdata, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Merge with IISCLOSE
    if not iisclose.is_empty():
        lnnote_merged = lnnote_merged.join(iisclose, on=['ACCTNO', 'NOTENO'], how='left')
    
    print(f"  Merged LNNOTE: {len(lnnote_merged)} records")
    
    # Merge with IIS and previous NPL
    print("\nCalculating IIS movements...")
    
    iis_merged = iis
    
    if not iis_merged.is_empty():
        iis_merged = iis_merged.join(lnnote_merged, on=['ACCTNO', 'NOTENO'], how='left')
        iis_merged = iis_merged.join(prev_npl, on=['ACCTNO', 'NOTENO'], how='left')
        
        # Calculate IIS for each row
        iis_results = []
        for row in iis_merged.rows(named=True):
            result = calculate_iis(row, rep_vars['reptdate'])
            iis_results.append(result)
        
        iis_final = pl.DataFrame(iis_results)
        print(f"  IIS calculated: {len(iis_final)} records")
    else:
        iis_final = pl.DataFrame()
    
    # Generate reports
    print("\nGenerating reports...")
    
    # Define NPL types for filtering
    npl_types = ['N', 'O', 'P', 'E']
    
    # Report 1: Newly surfaced / Normalised / Fully settled (non-CGC)
    if not iis_final.is_empty():
        generate_iis_report(
            iis_final.filter(~pl.col('LOANTYPE').is_in([517,521,522,523,528]) &
                             pl.col('NPLIND').is_in(['N','O','P'])),
            "MONTHLY MOVEMENT OF INTEREST-IN-SUSPENSE",
            "NPL INDICATOR: N-NEWLY SURFACED, O-NORMALIZE AND P-SETTLE",
            f"{PATHS['OUTPUT']}IIS_REPORT1.txt"
        )
        print(f"  Report 1: IIS_REPORT1.txt")
        
        # Report 2: CGC/TUK loans
        generate_iis_report(
            iis_final.filter(pl.col('LOANTYPE').is_in([517,521,522,523,528]) &
                             pl.col('NPLIND').is_in(['N','O','P'])),
            "MONTHLY MOVEMENT FOR NEWLY SURFACED/NORMALISED/FULLY SETTLED NON-PERFORMING CGC/TUK LOANS",
            "NPL INDICATOR: N-NEWLY SURFACED, O-NORMALIZE AND P-SETTLE",
            f"{PATHS['OUTPUT']}IIS_REPORT2.txt",
            columns=['BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'LOANTYPE', 
                    'AGEING', 'CLASSNEW', 'BALANCE', 'NPLIND', 'LSTTRNAM']
        )
        print(f"  Report 2: IIS_REPORT2.txt")
        
        # Report 3: All NPL by branch (non-CGC)
        generate_iis_report(
            iis_final.filter(~pl.col('LOANTYPE').is_in([517,521,522,523,528]) &
                             pl.col('NPLIND').is_in(['E','N','O','P'])),
            "MONTHLY MOVEMENT OF INTEREST-IN-SUSPENSE FOR NON-PERFORMING TL/FL/HL/RC",
            "NPL INDICATOR: E-EXISTING,N-NEWLY SURFACED, O-NORMALIZE AND P-SETTLE",
            f"{PATHS['OUTPUT']}IIS_REPORT3.txt",
            group_by='BRANCH'
        )
        print(f"  Report 3: IIS_REPORT3.txt")
        
        # Report 4: CGC/TUK by branch
        generate_iis_report(
            iis_final.filter(pl.col('LOANTYPE').is_in([517,521,522,523,528]) &
                             pl.col('NPLIND').is_in(['E','N','O','P'])),
            "MONTHLY MOVEMENT FOR NON-PERFORMING CGC/TUK LOANS",
            "NPL INDICATOR: E-EXISTING,N-NEWLY SURFACED, O-NORMALIZE AND P-SETTLE",
            f"{PATHS['OUTPUT']}IIS_REPORT4.txt",
            group_by='BRANCH',
            columns=['BRANCH', 'ACCTNO', 'NOTENO', 'NAME', 'LOANTYPE', 
                    'AGEING', 'CLASSNEW', 'BALANCE', 'NPLIND', 'LSTTRNAM']
        )
        print(f"  Report 4: IIS_REPORT4.txt")
    
    # Load commitment data for realisable value
    print("\nCalculating realisable values...")
    
    lncomm = load_lncomm()
    
    # Merge LNNOTE with LNCOMM
    if not lnnote.is_empty() and not lncomm.is_empty():
        lnnote_comm = lnnote.sort(['ACCTNO', 'COMMNO'])
        lncomm = lncomm.sort(['ACCTNO', 'COMMNO'])
        
        loan_merged = lnnote_comm.join(lncomm, on=['ACCTNO', 'COMMNO'], how='left')
        
        # Fill nulls
        loan_merged = loan_merged.with_columns([
            pl.col('CCURAMT').fill_null(0),
            pl.col('CUSEDAMT').fill_null(0)
        ])
        
        # Calculate undrawn
        loan_merged = loan_merged.with_columns([
            (pl.col('CCURAMT') - pl.col('CUSEDAMT')).alias('UNDRAWN')
        ])
        
        print(f"  Loan with commitments: {len(loan_merged)} records")
        
        # Merge with IIS data for accounts > 8000000000
        if not iis_final.is_empty():
            iis_large = iis_final.filter(pl.col('ACCTNO') > 8000000000)
            
            if not iis_large.is_empty():
                iis_large = iis_large.sort(['ACCTNO', 'NOTENO'])
                loan_merged = loan_merged.sort(['ACCTNO', 'NOTENO'])
                
                iis_with_collateral = iis_large.join(loan_merged, on=['ACCTNO', 'NOTENO'], how='left')
                
                # Calculate realisable value
                realis_results = []
                for row in iis_with_collateral.rows(named=True):
                    result = calculate_realisable_value(row, rep_vars['reptdate'])
                    realis_results.append(result)
                
                realis_df = pl.DataFrame(realis_results)
                
                # Add realisable value back to main IIS
                realis_df = realis_df.select(['ACCTNO', 'NOTENO', 'REALISVL'])
                iis_final = iis_final.join(realis_df, on=['ACCTNO', 'NOTENO'], how='left')
                
                print(f"  Realisable values calculated for {len(realis_df)} records")
    
    # Split by cost centre (conventional vs Islamic)
    print("\nSplitting by cost centre...")
    
    if not iis_final.is_empty() and 'COSTCTR' in iis_final.columns:
        # Conventional (costctr < 3000 or > 3999, excluding 4043,4048)
        conv_mask = ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999)) & \
                    (~pl.col('COSTCTR').is_in([4043, 4048]))
        
        conventional = iis_final.filter(conv_mask).unique(subset=['ACCTNO', 'NOTENO'])
        islamic = iis_final.filter(~conv_mask).unique(subset=['ACCTNO', 'NOTENO'])
        
        conventional.write_parquet(f"{PATHS['NPL']}TOTIIS{rep_vars['reptmon']}.parquet")
        islamic.write_parquet(f"{PATHS['NPL']}NPLI.TOTIIS{rep_vars['reptmon']}.parquet")
        
        print(f"  Conventional: {len(conventional)} records")
        print(f"  Islamic: {len(islamic)} records")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if not iis_final.is_empty():
        total_balance = iis_final['BALANCE'].sum()
        total_iis = iis_final['TOTIIS'].sum() if 'TOTIIS' in iis_final.columns else 0
        total_iisr = iis_final['TOTIISR'].sum() if 'TOTIISR' in iis_final.columns else 0
        total_woff = iis_final['TOTWOF'].sum() if 'TOTWOF' in iis_final.columns else 0
        
        print(f"\nTotal Outstanding Balance: RM {total_balance:,.2f}")
        print(f"Total IIS: RM {total_iis:,.2f}")
        print(f"Total IIS Recognized: RM {total_iisr:,.2f}")
        print(f"Total IIS Written-off: RM {total_woff:,.2f}")
        
        if 'REALISVL' in iis_final.columns:
            total_realis = iis_final['REALISVL'].sum()
            print(f"Total Realisable Value: RM {total_realis:,.2f}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMIISA Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
