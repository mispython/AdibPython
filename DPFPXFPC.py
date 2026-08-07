"""
CAMV / FDMV Movement Reports
=============================
Python re-implementation of the original SAS program.

Changes from the previous conversion, per request:
  1. Inputs are read directly from SAS datasets (.sas7bdat) using pyreadstat,
     instead of pre-staged parquet files read via DuckDB.
  2. The MNITB.REPTDATE lookup dataset has been removed entirely. The
     reporting date is now derived programmatically as "yesterday"
     (datetime.now() - timedelta(days=1)), matching how REPTDATE was used
     downstream (to build the &REPTDAY/&REPTMON dataset-name suffix and the
     "AS AT" date shown on each report).
  3. Output is written as plain, semicolon-delimited text files (.txt)
     instead of .csv, mirroring the SAS FILE/PUT-based text output.
  4. BRCHCD. and DDCUSTCD. are now real lookups, ported from the supplied
     PBBELF.py (BRCHCD_MAP / format_brchcd) and PBBDPTFMT.py
     (_DDCUSTCD_MAPPINGS / ddcustcd_format) - the same two programs the
     original SAS pulled in via %INC PGM(PBBDPFMT,PBBELF);.

Notes / assumptions carried over from the SAS source (flagged inline):
  - CURCODE does not exist in either the CAMV or FDMV dataset in this
    environment (confirmed against the real extract's column list), so the
    "PRODUCT IN (400:411,420:431,432:434) AND CURCODE NE 'MYR'" test that
    drives the CAMFYI/CAMFYC split is applied using PRODUCT range only.
  - CUSTCD = PUT(CUSTCODE, DDCUSTCD.) is compared in SAS against the literal
    list (02,03,07,10,12,81,82,83,84). Read at face value those look like
    the 2-digit BNM institutional-customer codes (commercial banks, Islamic
    banks, merchant banks, foreign banking institutions, etc.), so this
    script excludes any record whose *formatted* CUSTCD equals one of those
    2-digit strings. Note that DDCUSTCD. actually consolidates several raw
    CUSTCODE values into shared buckets (e.g. both 2 and 3 format to '30',
    81-84 all format to '86'), so in practice this filter only ever matches
    CUSTCD == '12' (raw CUSTCODE 12, Merchant Banks) against this dataset's
    DDCUSTCD table - none of the other seven literals are ever produced by
    DDCUSTCD. as coded. That may be a long-standing quirk in the original
    SAS (SAS's automatic numeric-to-character conversion of unpadded
    literals wouldn't match a zero-padded format value at all), but this
    script preserves the literal 2-digit reading since it's the more
    sensible business interpretation of what the filter is meant to do.
  - FDMFYI / FDMFYC report a header column labelled "NETBALC" but the SAS
    PUT statement for those two datasets actually writes the NETBALF value
    on each detail line. That mismatch exists in the original SAS code and
    is intentionally preserved here rather than "fixed".
"""

import pyreadstat
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ISLAMIC_CUSTCODES = {77, 78, 95, 96}

# The literal exclusion list from the SAS source: "CUSTCD NOT IN
# (02,03,07,10,12,81,82,83,84)". Read as the 2-digit DDCUSTCD.-formatted
# codes - see the module docstring for why.
EXCLUDED_DDCUSTCD_CODES = {'02', '03', '07', '10', '12', '81', '82', '83', '84'}


# ============================================================================
# FORMAT: BRCHCD — Branch Code -> Branch Abbreviation
# Ported from PBBELF.py (BRCHCD_MAP / format_brchcd)
# ============================================================================

BRCHCD_MAP = {
    1: 'HOE', 7000: 'HOE', 7001: 'HOE', 7002: 'HOE', 7003: 'HOE', 7004: 'HOE',
    7005: 'HOE', 7006: 'HOE', 7007: 'HOE', 7008: 'HOE', 7009: 'HOE',
    8000: 'HOE', 8001: 'HOE', 8002: 'HOE', 8003: 'HOE', 8004: 'HOE',
    8005: 'HOE', 8006: 'HOE', 8007: 'HOE', 8008: 'HOE', 8009: 'HOE',
    9000: 'HOE', 9001: 'HOE', 9002: 'HOE', 9003: 'HOE', 9004: 'HOE',
    9994: 'HOE', 9995: 'HOE', 9996: 'HOE', 9998: 'HOE', 9999: 'HOE',
    3000: 'IBU', 3001: 'IBU', 3999: 'IBU',
    4000: 'IBU', 4001: 'IBU', 4002: 'IBU', 4003: 'IBU', 4004: 'IBU',
    4005: 'IBU', 4006: 'IBU', 4007: 'IBU', 4008: 'IBU', 4009: 'IBU',
    800: 'H01', 3800: 'H01', 801: 'H02', 3801: 'H02', 802: 'H03', 3802: 'H03',
    803: 'H04', 3803: 'H04', 804: 'H05', 3804: 'H05', 805: 'H06', 3805: 'H06',
    806: 'H07', 3806: 'H07', 807: 'H08', 3807: 'H08', 808: 'H09', 3808: 'H09',
    809: 'H10', 3809: 'H10', 811: 'H11', 3811: 'H11', 812: 'H12', 3812: 'H12',
    813: 'H13', 3813: 'H13', 814: 'H14', 3814: 'H14', 815: 'H15', 3815: 'H15',
    816: 'H16', 3816: 'H16', 817: 'H17', 3817: 'H17', 818: 'H18', 3818: 'H18',
    819: 'H19', 3819: 'H19', 820: 'H20', 3820: 'H20', 821: 'H21', 3821: 'H21',
    822: 'H22', 3822: 'H22', 823: 'H23', 3823: 'H23', 824: 'H24', 3824: 'H24',
    825: 'H25', 3825: 'H25', 826: 'H26', 3826: 'H26', 827: 'H27', 3827: 'H27',
    828: 'H28', 3828: 'H28', 844: 'H44', 3844: 'H44', 845: 'H45', 3845: 'H45',
    846: 'H46', 3846: 'H46', 847: 'H47', 3847: 'H47', 848: 'H48', 3848: 'H48',
    849: 'H49', 3849: 'H49', 850: 'H50', 3850: 'H50', 851: 'H51', 3851: 'H51',
    852: 'H52', 3852: 'H52', 853: 'H53', 3853: 'H53', 854: 'H54', 3854: 'H54',
    855: 'H55', 3855: 'H55', 856: 'H56', 3856: 'H56', 857: 'H57', 3857: 'H57',
    858: 'H58', 3858: 'H58', 859: 'H59', 3859: 'H59', 860: 'H60', 3860: 'H60',
    861: 'H61', 3861: 'H61', 862: 'H62', 3862: 'H62', 863: 'H63', 3863: 'H63',
    2: 'JSS', 3002: 'JSS', 3: 'JRC', 3003: 'JRC', 4: 'MLK', 3004: 'MLK',
    5: 'IMO', 3005: 'IMO', 6: 'PPG', 3006: 'PPG', 7: 'JBU', 3007: 'JBU',
    8: 'KTN', 3008: 'KTN', 9: 'JYK', 3009: 'JYK', 10: 'ASR', 3010: 'ASR',
    11: 'GRN', 3011: 'GRN', 12: 'PPH', 3012: 'PPH', 13: 'KBU', 3013: 'KBU',
    14: 'TMH', 3014: 'TMH', 15: 'KPG', 3015: 'KPG', 16: 'NLI', 3016: 'NLI',
    17: 'TPN', 3017: 'TPN', 18: 'PJN', 3018: 'PJN', 19: 'DUA', 3019: 'DUA',
    20: 'TCL', 3020: 'TCL', 21: 'BPT', 3021: 'BPT', 22: 'SMY', 3022: 'SMY',
    23: 'KMT', 3023: 'KMT', 24: 'RSH', 3024: 'RSH', 25: 'SAM', 3025: 'SAM',
    26: 'SPG', 3026: 'SPG', 27: 'NTL', 3027: 'NTL', 28: 'MUA', 3028: 'MUA',
    29: 'JRL', 3029: 'JRL', 30: 'KTU', 3030: 'KTU', 31: 'SKC', 3031: 'SKC',
    32: 'WSS', 3032: 'WSS', 33: 'KKU', 3033: 'KKU', 34: 'KGR', 3034: 'KGR',
    35: 'SSA', 3035: 'SSA', 36: 'SS2', 3036: 'SS2', 37: 'TSA', 3037: 'TSA',
    38: 'JKL', 3038: 'JKL', 39: 'KKG', 3039: 'KKG', 40: 'JSB', 3040: 'JSB',
    41: 'JIH', 3041: 'JIH', 42: 'BMM', 3042: 'BMM', 43: 'BTG', 3043: 'BTG',
    44: 'TWU', 3044: 'TWU', 45: 'SRB', 3045: 'SRB', 46: 'APG', 3046: 'APG',
    47: 'SGM', 3047: 'SGM', 48: 'MTK', 3048: 'MTK', 49: 'JLP', 3049: 'JLP',
    50: 'MRI', 3050: 'MRI', 51: 'SMG', 3051: 'SMG', 52: 'UTM', 3052: 'UTM',
    53: 'TMI', 3053: 'TMI', 54: 'BBB', 3054: 'BBB', 55: 'LBN', 3055: 'LBN',
    56: 'KJG', 3056: 'KJG', 57: 'SPI', 3057: 'SPI', 58: 'SBU', 3058: 'SBU',
    59: 'PKL', 3059: 'PKL', 60: 'BAM', 3060: 'BAM', 61: 'KLI', 3061: 'KLI',
    62: 'SDK', 3062: 'SDK', 63: 'GMS', 3063: 'GMS', 64: 'PDN', 3064: 'PDN',
    65: 'BHU', 3065: 'BHU', 66: 'BDA', 3066: 'BDA', 67: 'CMR', 3067: 'CMR',
    68: 'SAT', 3068: 'SAT', 69: 'BKI', 3069: 'BKI', 70: 'PSA', 3070: 'PSA',
    71: 'BCG', 3071: 'BCG', 72: 'PPR', 3072: 'PPR', 73: 'SPK', 3073: 'SPK',
    74: 'SIK', 3074: 'SIK', 75: 'CAH', 3075: 'CAH', 76: 'PRS', 3076: 'PRS',
    77: 'PLI', 3077: 'PLI', 78: 'SJA', 3078: 'SJA', 79: 'MSI', 3079: 'MSI',
    80: 'MLB', 3080: 'MLB', 81: 'SBH', 3081: 'SBH', 82: 'MCG', 3082: 'MCG',
    83: 'JBB', 3083: 'JBB', 84: 'PMS', 3084: 'PMS', 85: 'SST', 3085: 'SST',
    86: 'CLN', 3086: 'CLN', 87: 'MSG', 3087: 'MSG', 88: 'KUM', 3088: 'KUM',
    89: 'TPI', 3089: 'TPI', 90: 'BTL', 3090: 'BTL', 91: 'KUG', 3091: 'KUG',
    92: 'KLG', 3092: 'KLG', 93: 'EDU', 3093: 'EDU', 94: 'STP', 3094: 'STP',
    95: 'TIN', 3095: 'TIN', 96: 'SGK', 3096: 'SGK', 97: 'HSL', 3097: 'HSL',
    98: 'TCY', 3098: 'TCY', 102: 'PRJ', 3102: 'PRJ', 103: 'JJG', 3103: 'JJG',
    104: 'KKL', 3104: 'KKL', 105: 'KTI', 3105: 'KTI', 106: 'CKI', 3106: 'CKI',
    107: 'JLT', 3107: 'JLT', 108: 'BSI', 3108: 'BSI', 109: 'KSR', 3109: 'KSR',
    110: 'TJJ', 3110: 'TJJ', 111: 'AKH', 3111: 'AKH', 112: 'LDO', 3112: 'LDO',
    113: 'TML', 3113: 'TML', 114: 'BBA', 3114: 'BBA', 115: 'KNG', 3115: 'KNG',
    116: 'TRI', 3116: 'TRI', 117: 'KKI', 3117: 'KKI', 118: 'TMW', 3118: 'TMW',
    120: 'PIH', 3120: 'PIH', 121: 'PRA', 3121: 'PRA', 122: 'SKN', 3122: 'SKN',
    123: 'IGN', 3123: 'IGN', 124: 'S14', 3124: 'S14', 125: 'KJA', 3125: 'KJA',
    126: 'PTS', 3126: 'PTS', 127: 'TSM', 3127: 'TSM', 128: 'SGB', 3128: 'SGB',
    129: 'BSR', 3129: 'BSR', 130: 'PDG', 3130: 'PDG', 131: 'TMG', 3131: 'TMG',
    132: 'CKT', 3132: 'CKT', 133: 'PKG', 3133: 'PKG', 134: 'RPG', 3134: 'RPG',
    135: 'BSY', 3135: 'BSY', 136: 'TCS', 3136: 'TCS', 137: 'JPP', 3137: 'JPP',
    138: 'WMU', 3138: 'WMU', 139: 'JRT', 3139: 'JRT', 140: 'CPE', 3140: 'CPE',
    141: 'STL', 3141: 'STL', 142: 'KBD', 3142: 'KBD', 143: 'LDU', 3143: 'LDU',
    144: 'KHG', 3144: 'KHG', 145: 'BSD', 3145: 'BSD', 146: 'PSG', 3146: 'PSG',
    147: 'PNS', 3147: 'PNS', 148: 'PJO', 3148: 'PJO', 149: 'BFT', 3149: 'BFT',
    150: 'LMM', 3150: 'LMM', 151: 'SLY', 3151: 'SLY', 152: 'ATR', 3152: 'ATR',
    153: 'USJ', 3153: 'USJ', 154: 'BSJ', 3154: 'BSJ', 155: 'TTJ', 3155: 'TTJ',
    156: 'TMR', 3156: 'TMR', 157: 'BPJ', 3157: 'BPJ', 158: 'SPL', 3158: 'SPL',
    159: 'RLU', 3159: 'RLU', 160: 'MTH', 3160: 'MTH', 161: 'DGG', 3161: 'DGG',
    162: 'SEA', 3162: 'SEA', 163: 'JKA', 3163: 'JKA', 164: 'KBS', 3164: 'KBS',
    165: 'TKA', 3165: 'TKA', 166: 'PGG', 3166: 'PGG', 167: 'BBG', 3167: 'BBG',
    168: 'KLC', 3168: 'KLC', 169: 'CTD', 3169: 'CTD', 170: 'PJA', 3170: 'PJA',
    171: 'JMR', 3171: 'JMR', 172: 'TMJ', 3172: 'TMJ', 173: 'SCA', 3173: 'SCA',
    174: 'BBP', 3174: 'BBP', 175: 'LBG', 3175: 'LBG', 176: 'TPG', 3176: 'TPG',
    177: 'JRU', 3177: 'JRU', 178: 'MIN', 3178: 'MIN', 179: 'OUG', 3179: 'OUG',
    180: 'KBG', 3180: 'KBG', 181: 'SRO', 3181: 'SRO', 182: 'JPU', 3182: 'JPU',
    183: 'JCL', 3183: 'JCL', 184: 'JPN', 3184: 'JPN', 185: 'KCY', 3185: 'KCY',
    186: 'JTZ', 3186: 'JTZ', 188: 'PLT', 3188: 'PLT', 189: 'BNH', 3189: 'BNH',
    190: 'BTR', 3190: 'BTR', 191: 'KPT', 3191: 'KPT', 192: 'MRD', 3192: 'MRD',
    193: 'MKH', 3193: 'MKH', 194: 'SRK', 3194: 'SRK', 195: 'BWK', 3195: 'BWK',
    196: 'JHL', 3196: 'JHL', 197: 'TNM', 3197: 'TNM', 198: 'TDA', 3198: 'TDA',
    199: 'JTH', 3199: 'JTH', 201: 'PDA', 3201: 'PDA', 202: 'RWG', 3202: 'RWG',
    203: 'SJM', 3203: 'SJM', 204: 'BTW', 3204: 'BTW', 205: 'SNG', 3205: 'SNG',
    206: 'TBM', 3206: 'TBM', 207: 'BCM', 3207: 'BCM', 208: 'JSI', 3208: 'JSI',
    209: 'STW', 3209: 'STW', 210: 'TMM', 3210: 'TMM', 211: 'TPD', 3211: 'TPD',
    212: 'JMA', 3212: 'JMA', 213: 'JKB', 3213: 'JKB', 214: 'JGA', 3214: 'JGA',
    215: 'JKP', 3215: 'JKP', 216: 'SKI', 3216: 'SKI', 217: 'TMB', 3217: 'TMB',
    220: 'GHS', 3220: 'GHS', 221: 'TSK', 3221: 'TSK', 222: 'TDC', 3222: 'TDC',
    223: 'TRJ', 3223: 'TRJ', 224: 'JAH', 3224: 'JAH', 225: 'TIH', 3225: 'TIH',
    226: 'JPR', 3226: 'JPR', 227: 'KSB', 3227: 'KSB', 228: 'INN', 3228: 'INN',
    229: 'TSJ', 3229: 'TSJ', 230: 'SSH', 3230: 'SSH', 231: 'BBM', 3231: 'BBM',
    232: 'TMD', 3232: 'TMD', 233: 'BEN', 3233: 'BEN', 234: 'SRM', 3234: 'SRM',
    235: 'SBM', 3235: 'SBM', 236: 'UYB', 3236: 'UYB', 237: 'KLS', 3237: 'KLS',
    238: 'JKT', 3238: 'JKT', 239: 'KMY', 3239: 'KMY', 240: 'KAP', 3240: 'KAP',
    241: 'DJA', 3241: 'DJA', 242: 'TKK', 3242: 'TKK', 243: 'KKR', 3243: 'KKR',
    244: 'GRT', 3244: 'GRT', 245: 'BDR', 3245: 'BDR', 246: 'BGH', 3246: 'BGH',
    247: 'BPR', 3247: 'BPR', 249: 'TAI', 3249: 'TAI', 248: 'JTS', 3248: 'JTS',
    250: 'TEA', 3250: 'TEA', 251: 'KPR', 3251: 'KPR', 252: 'TMA', 3252: 'TMA',
    253: 'JTT', 3253: 'JTT', 254: 'KPH', 3254: 'KPH', 255: 'SBP', 3255: 'SBP',
    256: 'PBR', 3256: 'PBR', 257: 'RAU', 3257: 'RAU', 258: 'JTA', 3258: 'JTA',
    259: 'SAN', 3259: 'SAN', 260: 'KDN', 3260: 'KDN', 261: 'GMG', 3261: 'GMG',
    262: 'TCT', 3262: 'TCT', 263: 'BTA', 3263: 'BTA', 264: 'JBH', 3264: 'JBH',
    265: 'JAI', 3265: 'JAI', 266: 'JDK', 3266: 'JDK', 267: 'TDI', 3267: 'TDI',
    268: 'BBT', 3268: 'BBT', 269: 'MKA', 3269: 'MKA', 270: 'BPI', 3270: 'BPI',
    273: 'LHA', 3273: 'LHA', 277: 'WSU', 3277: 'WSU', 278: 'JPI', 3278: 'JPI',
    274: 'STG', 3274: 'STG', 275: 'MSL', 3275: 'MSL', 276: 'JAS', 3276: 'JAS',
    279: 'PTJ', 3279: 'PTJ', 280: 'KDA', 3280: 'KDA', 281: 'PLT', 3281: 'PLT',
    282: 'PTT', 3282: 'PTT', 283: 'PSE', 3283: 'PSE', 284: 'BSP', 3284: 'BSP',
    285: 'BMC', 3285: 'BMC', 286: 'BIH', 3286: 'BIH', 287: 'SUA', 3287: 'SUA',
    288: 'SPT', 3288: 'SPT', 289: 'TEE', 3289: 'TEE', 290: 'TDY', 3290: 'TDY',
    291: 'BSL', 3291: 'BSL', 292: 'BMJ', 3292: 'BMJ', 293: 'BSA', 3293: 'BSA',
    294: 'KKM', 3294: 'KKM', 295: 'BKR', 3295: 'BKR', 296: 'BJL', 3296: 'BJL',
    701: 'IKB', 3701: 'IKB', 702: 'IPJ', 3702: 'IPJ', 703: 'IWS', 3703: 'IWS',
    704: 'IJK', 3704: 'IJK',
}


def fmt_branch(branch_code):
    """PUT(BRANCH, BRCHCD.) - ported from PBBELF.format_brchcd."""
    if branch_code is None or pd.isna(branch_code):
        return ''
    code = int(branch_code)
    if code in range(7000, 9001) or code in range(9994, 10000) or code == 1:
        return 'HOE'
    if code in (3000, 3001, 3999) or code in range(4000, 5000):
        return 'IBU'
    return BRCHCD_MAP.get(code, str(code))


# ============================================================================
# FORMAT: DDCUSTCD — Demand Deposit Customer Code
# Ported from PBBDPTFMT.py (_DDCUSTCD_MAPPINGS / ddcustcd_format)
# ============================================================================

_DDCUSTCD_MAPPINGS = {
    1: '01', 2: '30', 3: '30', 4: '04', 5: '05', 6: '06', 10: '30', 11: '30',
    12: '12', 13: '13', 15: '62', 17: '17', 20: '30', 30: '30', 31: '31',
    32: '32', 33: '33', 34: '34', 35: '35', 36: '06', 37: '37', 38: '38',
    39: '39', 40: '40', 41: '41', 42: '42', 43: '43', 44: '44', 45: '45',
    46: '46', 47: '47', 48: '48', 49: '49', 50: '78', 51: '51', 52: '52',
    53: '53', 54: '54', 57: '57', 59: '59', 60: '62', 61: '61', 62: '62',
    63: '63', 64: '64', 65: '44', 66: '41', 67: '44', 68: '48', 69: '52',
    70: '71', 71: '71', 72: '72', 73: '73', 74: '74', 75: '75', 76: '78',
    77: '77', 78: '78', 79: '79', 80: '86', 81: '86', 82: '86', 83: '86',
    84: '86', 85: '86', 86: '86', 87: '87', 88: '88', 89: '89', 90: '90',
    91: '91', 92: '92', 95: '95', 96: '96', 98: '98', 99: '99',
}


def ddcustcd_format(cust_code):
    """PUT(CUSTCODE, DDCUSTCD.) - ported from PBBDPTFMT.ddcustcd_format."""
    if cust_code is None or pd.isna(cust_code):
        return '79'
    return _DDCUSTCD_MAPPINGS.get(int(cust_code), '79')


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

reptdate = datetime.now() - timedelta(days=1)
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


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    process_camv()
    process_fdmv()
    print(f"\nCompleted: 12 text reports generated in {OUTPUT_DIR}")
