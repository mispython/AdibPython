"""
PBBLNFMT - Python Format Library (Function-Based)
Converted from SAS PROC FORMAT

This module provides formatting functions and mappings equivalent to the original SAS formats.
Can be imported and used by other Python programs.

Usage:
    import pbblnfmt
    
    result = pbblnfmt.oddenom(32)  # Returns 'I'
    result = pbblnfmt.odprod(3)    # Returns '34180'
    result = pbblnfmt.mthpass(45)  # Returns '1'
"""

# ============================================================================
# ODDENOM - OD DENOMINATION OF DOMESTIC OR ISLAMIC
# ============================================================================
def oddenom(value):
    """OD DENOMINATION OF DOMESTIC OR ISLAMIC"""
    islamic_values = [
        32, 33, 60, 61, 62, 63, 64, 92, 93, 96, 81,
        70, 71, 73, 74, 7, 8, 46, 47, 48, 49, 45,
        13, 14, 15, 16, 17, 18, 19, 23, 24, 25, 20, 21
    ] + list(range(160, 170)) + list(range(182, 189))
    
    return 'I' if value in islamic_values else 'D'


# ============================================================================
# ODPROD - OD PRODUCT MAPPING
# ============================================================================
def odprod(value):
    """OD Product Mapping"""
    _odprod_map = {
        3: '34180', 4: '34180', 5: '34180', 6: '34180', 7: '34180', 8: '34180',
        9: '34180', 10: '34180', 11: '34180', 12: '34180', 13: '34180', 14: '34180',
        15: '34180', 16: '34180', 17: '34180', 18: '34180', 19: '34180', 20: '34180',
        21: '34180', 22: '34180', 23: '34180', 24: '34180', 25: '34180', 26: '34180',
        27: '34180', 30: '34180', 31: '34180', 32: '34180', 33: '34180', 34: '34240',
        35: '34180', 36: '34180', 37: '34180', 38: '34180', 39: '34180', 41: '34180',
        42: '34180', 43: '34180', 45: '34180', 46: '34180', 47: '34180', 48: '34180',
        49: '34180', 50: '34180', 51: '34180', 52: '34180', 53: '34180', 54: '34180',
        55: '34180', 56: '34180', 57: '34180', 58: '34180', 59: '34180', 60: '34180',
        61: '34180', 62: '34180', 63: '34180', 64: '34180', 65: '34180', 68: '34180',
        69: '34180', 70: '34180', 71: '34180', 73: '34180', 74: '34180', 75: '34180',
        76: '34180', 77: '34240', 78: '34240', 81: '34180', 82: '34180', 83: '34180',
        84: '34180', 85: '34180', 86: '34180', 87: '34180', 88: '34180', 89: '34180',
        90: '34180', 91: '34180', 92: '34180', 93: '34180', 94: '34180', 95: '34180',
        96: '34180', 97: '34180', 98: 'N', 100: '34180', 101: '34180', 102: '34180',
        103: '34180', 104: '33110', 105: '33110', 106: '34180', 107: 'N', 108: '34180',
        109: '34180', 110: '34180', 111: '34180', 112: '34180', 113: '34180', 114: '34180',
        115: '34180', 116: '34180', 117: '34180', 118: '34180', 119: '34180', 120: '34180',
        121: '34180', 122: '34180', 123: '34180', 124: '34180', 125: '34180', 126: 'N',
        127: 'N', 128: 'N', 129: 'N', 130: 'N', 131: '34180', 132: '34180', 133: '34240',
        134: '34240', 135: '34180', 136: 'N', 137: '34180', 138: '34180', 139: 'N',
        140: 'N', 141: 'N', 142: 'N', 143: 'N', 144: 'N', 145: 'N', 146: 'N', 147: 'N',
        148: 'N', 149: 'N', 150: '34180', 151: '34180', 152: '34180', 153: '34180',
        154: '34180', 155: '34180', 156: '34180', 157: '34180', 158: '34180', 159: '34180',
        160: '34180', 161: '34180', 162: '34180', 163: '34180', 164: '34180', 165: '34180',
        166: '34180', 167: '34180', 168: '34180', 169: '34180', 170: '34180', 171: 'N',
        172: 'N', 173: 'N', 174: '34180', 175: '34180', 176: '34180', 177: '34240',
        178: '34240', 180: '34180', 181: '34180', 182: '34180', 183: '34180', 184: '34180',
        185: '34180', 186: '34180', 187: '34180', 188: '34180', 190: '34180', 191: '34180',
        192: '34180', 193: '34180', 194: '34180', 195: '34180', 196: '34180', 197: '34180',
        198: '34180', 473: 'N', 474: 'N', 475: 'N', 476: 'N', 477: 'N', 478: 'N',
        479: 'N', 549: 'N', 550: 'N'
    }
    return _odprod_map.get(value, '34180')


# ============================================================================
# LNDENOM - LOAN DENOMINATION
# ============================================================================
def lndenom(value):
    """LOAN DENOMINATION OF DOMESTIC OR ISLAMIC"""
    islamic_values = (
        [100, 119, 120, 122, 126, 127, 128, 129, 130, 131, 132, 185, 169, 134, 135, 136,
         138, 139, 140, 141, 142, 143, 170, 180, 181, 182, 183, 101, 147, 148, 173, 174,
         159, 160, 161, 162, 164, 165, 179, 146, 184, 191, 197, 199, 124, 145, 144, 163,
         186, 187, 102, 103, 104, 105, 106, 107, 108, 188, 189, 190, 137, 973, 400, 401,
         402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417,
         418, 419, 427, 428, 429, 445, 446, 448] +
        list(range(110, 119)) + list(range(192, 197)) + list(range(152, 159)) +
        list(range(420, 423)) + list(range(461, 471)) + list(range(430, 440)) +
        list(range(440, 445)) + list(range(650, 700)) + list(range(471, 499)) +
        list(range(851, 900))
    )
    return 'I' if value in islamic_values else 'D'


# ============================================================================
# MTHPASS - MONTHS PAST DUE
# ============================================================================
def mthpass(days):
    """Convert days past due to month category"""
    if 0 <= days <= 30:
        return '0'
    elif 31 <= days <= 59:
        return '1'
    elif 60 <= days <= 89:
        return '2'
    elif 90 <= days <= 121:
        return '3'
    elif 122 <= days <= 151:
        return '4'
    elif 152 <= days <= 182:
        return '5'
    elif 183 <= days <= 213:
        return '6'
    elif 214 <= days <= 243:
        return '7'
    elif 244 <= days <= 273:
        return '8'
    elif 274 <= days <= 303:
        return '9'
    elif 304 <= days <= 333:
        return '10'
    elif 334 <= days <= 364:
        return '11'
    elif 365 <= days <= 394:
        return '12'
    elif 395 <= days <= 424:
        return '13'
    elif 425 <= days <= 456:
        return '14'
    elif 457 <= days <= 486:
        return '15'
    elif 487 <= days <= 516:
        return '16'
    elif 517 <= days <= 547:
        return '17'
    elif 548 <= days <= 577:
        return '18'
    elif 578 <= days <= 608:
        return '19'
    elif 609 <= days <= 638:
        return '20'
    elif 639 <= days <= 668:
        return '21'
    elif 669 <= days <= 698:
        return '22'
    elif 699 <= days <= 729:
        return '23'
    else:
        return '24'


# ============================================================================
# NDAYS - Convert months to days (INVALUE)
# ============================================================================
def ndays(days):
    """Convert days range to month number"""
    if days <= 30:
        return 0
    elif 31 <= days <= 59:
        return 1
    elif 60 <= days <= 89:
        return 2
    elif 90 <= days <= 121:
        return 3
    elif 122 <= days <= 151:
        return 4
    elif 152 <= days <= 182:
        return 5
    elif 183 <= days <= 213:
        return 6
    elif 214 <= days <= 243:
        return 7
    elif 244 <= days <= 273:
        return 8
    elif 274 <= days <= 303:
        return 9
    elif 304 <= days <= 333:
        return 10
    elif 334 <= days <= 364:
        return 11
    elif 365 <= days <= 394:
        return 12
    elif 395 <= days <= 424:
        return 13
    elif 425 <= days <= 456:
        return 14
    elif 457 <= days <= 486:
        return 15
    elif 487 <= days <= 516:
        return 16
    elif 517 <= days <= 547:
        return 17
    elif 548 <= days <= 577:
        return 18
    elif 578 <= days <= 608:
        return 19
    elif 609 <= days <= 638:
        return 20
    elif 639 <= days <= 668:
        return 21
    elif 669 <= days <= 698:
        return 22
    elif 699 <= days <= 729:
        return 23
    else:
        return 24


# ============================================================================
# STATECD - STATE CODE MAPPING
# ============================================================================
def statecd(value):
    """State Code Mapping"""
    value_str = str(value).zfill(6)
    
    state_map = {
        '1': 'J', '01': 'J', '001': 'J', '0001': 'J', '00001': 'J', '000001': 'J',
        '2': 'K', '02': 'K', '002': 'K', '0002': 'K', '00002': 'K', '000002': 'K',
        '3': 'D', '03': 'D', '003': 'D', '0003': 'D', '00003': 'D', '000003': 'D',
        '4': 'M', '04': 'M', '004': 'M', '0004': 'M', '00004': 'M', '000004': 'M',
        '5': 'N', '05': 'N', '005': 'N', '0005': 'N', '00005': 'N', '000005': 'N',
        '6': 'C', '06': 'C', '006': 'C', '0006': 'C', '00006': 'C', '000006': 'C',
        '7': 'P', '07': 'P', '007': 'P', '0007': 'P', '00007': 'P', '000007': 'P',
        '8': 'A', '08': 'A', '008': 'A', '0008': 'A', '00008': 'A', '000008': 'A',
        '9': 'R', '09': 'R', '009': 'R', '0009': 'R', '00009': 'R', '000009': 'R',
        '10': 'S', '010': 'S', '0010': 'S', '00010': 'S', '000010': 'S',
        '11': 'Q', '011': 'Q', '0011': 'Q', '00011': 'Q', '000011': 'Q',
        '12': 'B', '012': 'B', '0012': 'B', '00012': 'B', '000012': 'B',
        '13': 'T', '013': 'T', '0013': 'T', '00013': 'T', '000013': 'T',
        '14': 'W', '014': 'W', '0014': 'W', '00014': 'W', '000014': 'W',
        '16': 'W', '016': 'W', '0016': 'W', '00016': 'W', '000016': 'W',
        '15': 'L', '015': 'L', '0015': 'L', '00015': 'L', '000015': 'L'
    }
    
    return state_map.get(str(value), ' ')


# ============================================================================
# RISKCD - RISK CODE MAPPING
# ============================================================================
def riskcd(value):
    """Risk Code Mapping"""
    risk_map = {
        '2': '34902', '02': '34902', '002': '34902', '0002': '34902',
        '3': '34903', '03': '34903', '003': '34903', '0003': '34903',
        '4': '34904', '04': '34904', '004': '34904', '0004': '34904'
    }
    return risk_map.get(str(value), ' ')


# ============================================================================
# BUSIND - BUSINESS/INDIVIDUAL INDICATOR
# ============================================================================
def busind(value):
    """Business/Individual Indicator"""
    value_str = str(value).zfill(2)
    
    business_codes = [
        '01', '02', '03', '04', '05', '06',
        '11', '12', '13', '17',
        '30', '31', '32', '33', '34', '35',
        '37', '38', '39', '40', '45', '57', '59',
        '61', '62', '63', '64',
        '66', '67', '68', '69',
        '71', '72', '73', '74', '75'
    ]
    
    individual_codes = ['77', '78', '79', '95', '96']
    
    if value_str in business_codes:
        return 'BUS'
    elif value_str in individual_codes:
        return 'IND'
    else:
        return 'IND'


# ============================================================================
# APPRLIMT - APPROVAL LIMIT
# ============================================================================
def apprlimt(amount):
    """Approval Limit Category"""
    if amount < 100000:
        return '30511'
    elif amount < 500000:
        return '30512'
    elif amount < 1000000:
        return '30513'
    elif amount < 5000000:
        return '30514'
    elif amount < 20000000:
        return '30515'
    elif amount < 50000000:
        return '30516'
    else:
        return '30519'


# ============================================================================
# LOANSIZE - LOAN SIZE CATEGORY
# ============================================================================
def loansize(amount):
    """Loan Size Category"""
    if amount < 100000:
        return '80511'
    elif amount < 500000:
        return '80512'
    elif amount < 1000000:
        return '80513'
    elif amount < 5000000:
        return '80514'
    elif amount < 20000000:
        return '80515'
    elif amount < 50000000:
        return '80516'
    else:
        return '80519'


# ============================================================================
# LNORMT - LOAN ORIGINAL MATURITY
# ============================================================================
def lnormt(months):
    """Loan Original Maturity Category"""
    if months < 1:
        return '12'
    elif months < 2:
        return '13'
    elif months < 3:
        return '14'
    elif months < 6:
        return '15'
    elif months < 9:
        return '16'
    elif months < 12:
        return '17'
    elif months < 15:
        return '21'
    elif months < 18:
        return '22'
    elif months < 24:
        return '23'
    elif months < 36:
        return '24'
    elif months < 48:
        return '25'
    elif months < 60:
        return '26'
    elif months < 120:
        return '31'
    elif months < 180:
        return '32'
    else:
        return '33'


# ============================================================================
# LNRMMT - LOAN REMAINING MATURITY
# ============================================================================
def lnrmmt(months):
    """Loan Remaining Maturity Category"""
    if months < 0:
        return '51'
    elif months < 1:
        return '52'
    elif months < 2:
        return '53'
    elif months < 3:
        return '54'
    elif months < 6:
        return '55'
    elif months < 9:
        return '56'
    elif months < 12:
        return '57'
    elif months < 24:
        return '61'
    elif months < 36:
        return '62'
    elif months < 48:
        return '63'
    elif months < 60:
        return '64'
    elif months < 120:
        return '71'
    elif months < 180:
        return '72'
    else:
        return '73'


# ============================================================================
# COLLCD - COLLATERAL CODE
# ============================================================================
def collcd(value):
    """Collateral Code Mapping"""
    value_str = str(value).zfill(2)
    
    collateral_map = {
        '1': '30570', '01': '30570',
        '2': '30570', '02': '30570',
        '3': '30570', '03': '30570',
        '5': '30570', '05': '30570',
        '6': '30570', '06': '30570',
        '7': '30570', '07': '30570',
        '8': '30520', '08': '30520',
        '9': '30570', '09': '30570',
        '10': '30570',
        '11': '30570',
        '12': '30570',
        '13': '30530',
        '14': '30570',
        '15': '30540',
        '20': '30580',
        '21': '30570',
        '22': '30580',
        '23': '30580',
        '41': '30570',
        '42': '30570',
        '43': '30570',
        '50': '30570',
        '51': '30570',
        '52': '30570',
        '53': '30570',
        '54': '30570',
        '55': '30570',
        '56': '30570',
        '57': '30570',
        '60': '30570',
        '61': '30570'
    }
    
    return collateral_map.get(value_str, '30570')


# ============================================================================
# ARRCLASS - ARREARS CLASSIFICATION
# ============================================================================
def arrclass(value):
    """Arrears Classification"""
    arrclass_map = {
        1: '0 - < 1 MTH   ',
        2: '1 - < 2 MTH   ',
        3: '2 - < 3 MTH   ',
        4: '3 - < 4 MTH   ',
        5: '4 - < 5 MTH   ',
        6: '5 - < 6 MTH   ',
        7: '6 - < 7 MTH   ',
        8: '7 - < 8 MTH   ',
        9: '8 - < 9 MTH   ',
        10: '9 - < 12 MTH  ',
        11: '12 - < 18 MTH ',
        12: '18 - < 24 MTH ',
        13: '24 - < 36 MTH ',
        14: '36 MTH & ABOVE',
        15: 'DEFICIT       '
    }
    return arrclass_map.get(value, '')


# ============================================================================
# DELQDES - DELINQUENCY DESCRIPTION
# ============================================================================
def delqdes(value):
    """Delinquency Description"""
    delq_map = {
        '  ': 'NO LEGAL ACTION TAKEN',
        '09': 'LNOD/RECALL ISSUED',
        '10': 'SUMMON/WRIT FILED',
        '11': 'JUDGEMENT ORDER',
        '12': 'BANKRUPTCY',
        '13': 'CHARGING ORDER',
        '14': 'GARNISHEE ORDER',
        '15': 'WRIT OF SEIZURE AND SALE',
        '16': 'PROHIBITORY ORDER',
        '17': 'WINDING-UP',
        '18': 'AUCTION',
        '19': 'JUDGEMENT DEBTOR SUMMONS',
        '20': 'RECEIVER/SECTION 176',
        '21': 'SETTLED/DISCHARGED'
    }
    return delq_map.get(str(value), '')


# ============================================================================
# Helper function to check if value is in range
# ============================================================================
def _in_range(value, start, end):
    """Check if value is in range (inclusive)"""
    try:
        return start <= int(value) <= end
    except (ValueError, TypeError):
        return False


# ============================================================================
# Macro Variables (as constants)
# ============================================================================
MOREPLAN = [116, 119, 234, 235, 236, 242]
MOREISLM = [116, 119]
HP = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]
HPD = [128, 130, 131, 132, 380, 381, 700, 705, 720, 725]
AITAB = [128, 130, 131, 132]
HOMEIS = [113, 115, 117, 118]
HOMECV = [227, 228, 230, 231, 237, 238, 239, 240, 241]
SWIFTIS = [126, 127]
SWIFTCV = [359]
FCY = [800, 801, 802, 803, 804, 805, 806, 807, 808, 816, 817,
       809, 810, 811, 812, 813, 814, 815, 851, 852, 853, 854,
       855, 856, 857, 858, 859, 860]


# ============================================================================
# Main test function
# ============================================================================
if __name__ == "__main__":
    # Test examples
    print("Testing PBBLNFMT Functions:")
    print(f"oddenom(32) = {oddenom(32)}")  # Should return 'I'
    print(f"oddenom(1) = {oddenom(1)}")    # Should return 'D'
    print(f"odprod(3) = {odprod(3)}")      # Should return '34180'
    print(f"mthpass(45) = {mthpass(45)}")  # Should return '1'
    print(f"statecd('01') = {statecd('01')}")  # Should return 'J'
    print(f"riskcd('2') = {riskcd('2')}")  # Should return '34902'
    print(f"busind('01') = {busind('01')}")  # Should return 'BUS'
    print(f"apprlimt(150000) = {apprlimt(150000)}")  # Should return '30512'
    print(f"loansize(250000) = {loansize(250000)}")  # Should return '80512'
    print(f"arrclass(5) = {arrclass(5)}")  # Should return '4 - < 5 MTH   '
