import pandas as pd
import numpy as np

# DAYR - Convert days to period numbers
def dayr_mapping(days):
    """
    Maps number of days to period numbers (0-24)
    Similar to SAS INVALUE DAYR format
    """
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
    else:  # 730 and above
        return 24

# Vectorized version for pandas Series
def dayr_vectorized(days_series):
    """
    Vectorized version for pandas Series
    """
    conditions = [
        days_series <= 30,
        (days_series >= 31) & (days_series <= 59),
        (days_series >= 60) & (days_series <= 89),
        (days_series >= 90) & (days_series <= 121),
        (days_series >= 122) & (days_series <= 151),
        (days_series >= 152) & (days_series <= 182),
        (days_series >= 183) & (days_series <= 213),
        (days_series >= 214) & (days_series <= 243),
        (days_series >= 244) & (days_series <= 273),
        (days_series >= 274) & (days_series <= 303),
        (days_series >= 304) & (days_series <= 333),
        (days_series >= 334) & (days_series <= 364),
        (days_series >= 365) & (days_series <= 394),
        (days_series >= 395) & (days_series <= 424),
        (days_series >= 425) & (days_series <= 456),
        (days_series >= 457) & (days_series <= 486),
        (days_series >= 487) & (days_series <= 516),
        (days_series >= 517) & (days_series <= 547),
        (days_series >= 548) & (days_series <= 577),
        (days_series >= 578) & (days_series <= 608),
        (days_series >= 609) & (days_series <= 638),
        (days_series >= 639) & (days_series <= 668),
        (days_series >= 669) & (days_series <= 698),
        (days_series >= 699) & (days_series <= 729),
        days_series >= 730
    ]
    choices = list(range(25))
    return np.select(conditions, choices, default=np.nan)

# $DIRCT mapping - Direction
DIRCT_MAPPING = {
    'MDL': 'D', 'MLL': 'D', 'MOL': 'D', 'MOF': 'D', 'MCL': 'D', 'MCF': 'D',
    'PEF': 'D', 'PCE': 'D', 'PCB': 'D', 'TRX': 'D', 'TRL': 'D', 'TRC': 'D',
    'BAX': 'D', 'BAL': 'D', 'FBP': 'D', 'FBD': 'D', 'FBC': 'D', 'IEF': 'D',
    'TFL': 'D', 'TML': 'D', 'TFC': 'D', 'TMC': 'D', 'TFO': 'D', 'TMO': 'D',
    'TLL': 'D', 'TNL': 'D', 'TLC': 'D', 'TNC': 'D', 'TNO': 'D', 'PBU': 'D',
    'PBR': 'D', 'TLZ': 'D', 'TLQ': 'D', 'TLS': 'D', 'TLX': 'D', 'TLY': 'D',
    'TLO': 'D', 'BAP': 'D', 'BAI': 'D', 'BAS': 'D', 'BAE': 'D', 'PBA': 'D',
    'FRL': 'D', 'TRF': 'D', 'FAS': 'D', 'FAU': 'D', 'PFU': 'D', 'FDS': 'D',
    'FDU': 'D', 'PFD': 'D', 'FCL': 'D', 'FTB': 'D', 'FTL': 'D', 'FTI': 'D',
    'DAS': 'D', 'DAU': 'D', 'PAU': 'D', 'DDS': 'D', 'DDU': 'D', 'DDT': 'D',
    'PDU': 'D', 'PDT': 'D', 'ITB': 'D', 'PTB': 'D', 'POS': 'D', 'PRO': 'D',
    'BRM': 'D', 'BRN': 'D', 'PBO': 'D', 'PBD': 'D', 'PBQ': 'D', 'PBZ': 'D',
    'PFT': 'D', 'VAL': 'D', 'DIL': 'D', 'FIL': 'D', 'PRE': 'D', 'PCR': 'D',
    'BRF': 'D', 'BRL': 'D', 'PUM': 'D', 'TFI': 'D', 'TBI': 'D', 'TLI': 'D',
    'TXI': 'D', 'BPI': 'D', 'BII': 'D', 'BSI': 'D', 'BEI': 'D', 'PTR': 'D',
    'PRU': 'D', 'PBI': 'D', 'PCP': 'D', 'MFL': 'D',
    'IFS': 'I', 'IFD': 'I', 'IFU': 'I', 'IFO': 'I', 'ILS': 'I', 'ILB': 'I',
    'ILU': 'I', 'ILL': 'I', 'SFC': 'I', 'SLC': 'I', 'TFR': 'I', 'TLR': 'I',
    'BFC': 'I', 'BLC': 'I', 'DLC': 'I', 'RFC': 'I', 'RLC': 'I', 'PLC': 'I',
    'ALC': 'I', 'SGL': 'I', 'SGC': 'I', 'APG': 'I', 'BGF': 'I', 'BGT': 'I',
    'BGP': 'I', '190': 'I', '200': 'I', 'BUF': 'I', 'BUL': 'I', 'BRA': 'I',
    'FSI': 'I', 'FUI': 'I', 'LSI': 'I', 'LUI': 'I', 'SLI': 'I', 'SCI': 'I',
    'GTI': 'I', 'GPI': 'I', 'GFI': 'I', 'UFI': 'I', 'UDI': 'I', 'BGG': 'I',
    'GGI': 'I'
}

# Function to apply $DIRCT mapping
def map_direct(code):
    """Maps product code to Direction (D or I)"""
    return DIRCT_MAPPING.get(code, ' ')

# $LIAB mapping - Liability codes
LIAB_MAPPING = {
    'IFS': '34810', 'IFD': '34810', 'IFU': '34810', 'IFO': '34810',
    'ILS': '34810', 'ILB': '34810', 'ILU': '34810', 'ILL': '34810',
    'ALC': '34810', 'TFR': '34810', 'BLC': '34810', 'DLC': '34810',
    'RLC': '34810', 'PLC': '34810', 'BUF': '34810', 'UFI': '34810',
    'FSI': '34810', 'FUI': '34810', 'LSI': '34810', 'LSU': '34810',
    'IUF': '34810', 'IUL': '34810', 'UDI': '34810', 'BUL': '34810',
    'BRA': '34810', '190': '34810', '200': '34810', 'BFC': '34810',
    'RFC': '34810', 'LUI': '34810', 'LUO': '34810', 'LSO': '34810',
    'FUO': '34810', 'FSO': '34810',
    'SFC': '34821',
    'SLC': '34822',
    'BRF': '34480', 'BRM': '34480', 'BRL': '34480', 'BRN': '34480',
    'PBU': '34480', 'PBR': '34480', 'PUM': '34480', 'PFT': '34480',
    'PCR': '34480', 'PEF': '34480', 'PCE': '34480', 'PCP': '34480',
    'FRL': '34480', 'TRF': '34480', 'PRU': '34480', 'FTL': '34480',
    'FTI': '34480',
    'TFL': '34440', 'TML': '34440', 'TFC': '34440', 'TMC': '34440',
    'TFO': '34440', 'TMO': '34440', 'TLL': '34440', 'TNL': '34440',
    'TLC': '34440', 'TNC': '34440', 'TLO': '34440', 'TNO': '34440',
    'TLF': '34440', 'TLZ': '34440', 'TLQ': '34440', 'TLS': '34440',
    'TLX': '34440', 'TLY': '34440', 'TLW': '34440', 'TLV': '34440',
    'TFI': '34440', 'TBI': '34440', 'TLI': '34440', 'TXI': '34440',
    'PTR': '34440',
    'FAS': '34422', 'FAU': '34422', 'FDS': '34422', 'FDU': '34422',
    'FCL': '34422', 'FTB': '34422', 'FFS': '34422', 'FFU': '34422',
    'FCS': '34422', 'FCU': '34422', 'FFL': '34422',
    'DAS': '34421', 'DAU': '34421', 'DDS': '34421', 'DDU': '34421',
    'DDT': '34421', 'ITB': '34421', 'PAU': '34421', 'PDU': '34421',
    'PDT': '34421', 'PTB': '34421', 'PFU': '34421', 'PFD': '34421',
    'BAP': '34470', 'BAI': '34470', 'BAS': '34470', 'BAE': '34470',
    'PBA': '34470', 'PBO': '34470', 'PBZ': '34470', 'PBD': '34470',
    'PBQ': '34470', 'BPI': '34470', 'BII': '34470', 'BSI': '34470',
    'BEI': '34470', 'PBI': '34470',
    'MDL': '34411', 'MOL': '34411', 'MLL': '34411', 'MFL': '34411',
    'VAL': '34411', 'DIL': '34411', 'FIL': '34411', 'PRE': '34411',
    'MOF': '34411',
    'POS': '34412', 'PRO': '34412',
    'SGL': '34850', 'SGC': '34850', 'ISL': '34850', 'ISC': '34850',
    'SLI': '34850', 'SCI': '34850',
    'BGT': '34840', 'BGP': '34840', 'GTI': '34840', 'GPI': '34840',
    'BGG': '34831', 'GGI': '34831',
    'BGF': '34832', 'APG': '34832', 'GFI': '34832',
    'MCF': '34490', 'IEF': '34490'
}

def map_liab(code):
    """Maps product code to Liability code"""
    return LIAB_MAPPING.get(code, '99999')

# $BTFCEPT mapping - BTFCEPT codes
BTFCEPT_MAPPING = {
    'TBI': '12', 'TXI': '12',
    'BSI': '19',
    'FSI': '22', 'FUI': '22', 'LSI': '22', 'LUI': '22', 'UFI': '22',
    'UDI': '22', 'LUO': '22', 'LSO': '22', 'FUO': '22', 'FSO': '22',
    'SLI': '23', 'SCI': '23', 'GFI': '23', 'GPI': '23', 'GTI': '23',
    'GGI': '23',
    'BPI': '35',
    'TFI': '36', 'TLI': '36', 'PRU': '36',
    'BII': '49', 'BEI': '49', 'PBI': '49'
}

def map_btfcept(code):
    """Maps product code to BTFCEPT code"""
    return BTFCEPT_MAPPING.get(code, '99')

# $PRCTYPE mapping - Pricing type
PRCTYPE_MAPPING = {
    'FSI': '00', 'FUI': '00', 'LSI': '00', 'LUI': '00', 'FSO': '00',
    'FUO': '00', 'LSO': '00', 'LUO': '00', 'UFI': '00', 'UDI': '00',
    'GTI': '00', 'GPI': '00', 'GFI': '00', 'SLI': '00', 'SCI': '00',
    'ALC': '00', 'IFD': '00', 'IFO': '00', 'ILS': '00', 'ILU': '00',
    'IFS': '00', 'IFU': '00', 'ILB': '00', 'ILL': '00', 'SFC': '00',
    'SLC': '00', 'BFC': '00', 'BLC': '00', 'DLC': '00', 'RFC': '00',
    'RLC': '00', 'FAS': '00', 'FAU': '00', 'FDS': '00', 'FDU': '00',
    'FCL': '00', 'FFS': '00', 'FFU': '00', 'FCS': '00', 'FCU': '00',
    'FFL': '00', 'FTB': '00', 'BUF': '00', 'BUL': '00', 'BGT': '00',
    'BGP': '00', 'BGF': '00', 'APG': '00', 'SGL': '00', 'SGC': '00',
    'BGG': '00', 'GGI': '00',
    'TFI': '41', 'TLI': '41', 'TFL': '41', 'TLL': '41', 'TLZ': '41',
    'DAS': '41', 'DAU': '41', 'DDS': '41', 'DDU': '41', 'ITB': '41',
    'BRF': '41', 'BRL': '41', 'PBU': '41', 'PDU': '41', 'PRE': '41',
    'PRO': '41', 'PBA': '41', 'PBZ': '41',
    'BPI': '53', 'BSI': '53', 'BII': '53', 'BEI': '53', 'BAS': '53',
    'BAE': '53', 'BAP': '53', 'BAI': '53',
    'FTI': '68', 'FTL': '68',
    'PBI': '79', 'PRU': '79', 'VAL': '79', 'DIL': '79', 'FIL': '79',
    'POS': '79'
}

def map_prctype(code):
    """Maps product code to PRCTYPE"""
    return PRCTYPE_MAPPING.get(code, '99')

# $PRCTYPESFS mapping - Pricing type for SFS
PRCTYPESFS_MAPPING = {
    'FSI': '00', 'FUI': '00', 'LSI': '00', 'LUI': '00', 'FSO': '00',
    'FUO': '00', 'LSO': '00', 'LUO': '00', 'UFI': '00', 'UDI': '00',
    'GTI': '00', 'GPI': '00', 'GFI': '00', 'SLI': '00', 'SCI': '00',
    'ALC': '00', 'IFD': '00', 'IFO': '00', 'ILS': '00', 'ILU': '00',
    'IFS': '00', 'IFU': '00', 'ILB': '00', 'ILL': '00', 'SFC': '00',
    'SLC': '00', 'BFC': '00', 'BLC': '00', 'DLC': '00', 'RFC': '00',
    'RLC': '00', 'FAS': '00', 'FAU': '00', 'FDS': '00', 'FDU': '00',
    'FCL': '00', 'FFS': '00', 'FFU': '00', 'FCS': '00', 'FCU': '00',
    'FFL': '00', 'FTB': '00', 'BUF': '00', 'BUL': '00', 'BGT': '00',
    'BGP': '00', 'BGF': '00', 'APG': '00', 'SGL': '00', 'SGC': '00',
    'BGG': '00', 'GGI': '00',
    'DAS': '41', 'DAU': '41', 'DDS': '41', 'DDU': '41', 'ITB': '41',
    'PBU': '41', 'PDU': '41', 'PRE': '41', 'PRO': '41',
    'FTI': '68', 'FTL': '68',
    'PBI': '79', 'PRU': '79', 'VAL': '79', 'DIL': '79', 'FIL': '79',
    'POS': '79',
    'TFI': '59', 'TLI': '59', 'BPI': '59', 'BSI': '59', 'BII': '59',
    'BEI': '59', 'TFL': '59', 'TLL': '59', 'TLZ': '59', 'BRF': '59',
    'BRL': '59', 'PBA': '59', 'PBZ': '59', 'BAS': '59', 'BAE': '59',
    'BAP': '59', 'BAI': '59'
}

def map_prctypesfs(code):
    """Maps product code to PRCTYPESFS"""
    return PRCTYPESFS_MAPPING.get(code, '99')

# $NSRSLIAB mapping - NSRS liability codes
NSRSLIAB_MAPPING = {
    'TFL': '34440', 'TML': '34440', 'TFC': '34440', 'TMC': '34440',
    'TFO': '34440', 'TMO': '34440', 'TLL': '34440', 'TNL': '34440',
    'TLC': '34440', 'TNC': '34440', 'TLO': '34440', 'TNO': '34440',
    'TLF': '34440', 'TLQ': '34440', 'TLS': '34440', 'TLV': '34440',
    'TLW': '34440', 'TLX': '34440', 'TLY': '34440', 'TLZ': '34440',
    'TRF': '34440', 'TFI': '34440', 'TBI': '34440', 'TLI': '34440',
    'TXI': '34440',
    'BAE': '34473', 'BEI': '34473',
    'BAI': '34474', 'BII': '34474',
    'BAP': '34476', 'BAS': '34476', 'PBA': '34476', 'PBO': '34476',
    'PBD': '34476', 'PBQ': '34476', 'PBZ': '34476', 'BPI': '34476',
    'BSI': '34476', 'PBI': '34476',
    'FAS': '34479', 'FAU': '34479', 'FDS': '34479', 'FDU': '34479',
    'FCL': '34479', 'FTB': '34479', 'DAS': '34479', 'DAU': '34479',
    'DDS': '34479', 'DDU': '34479', 'DDT': '34479', 'ITB': '34479',
    'IEF': '34479', 'MDL': '34479', 'MOL': '34479', 'MLL': '34479',
    'MFL': '34479', 'MOF': '34479', 'MCF': '34479', 'PDU': '34479',
    'PCE': '34479', 'PCP': '34479', 'PEF': '34479', 'PFD': '34479',
    'PFU': '34479', 'PTB': '34479', 'FFS': '34479', 'FFU': '34479',
    'FCS': '34479', 'FCU': '34479', 'FFL': '34479',
    'BRF': '34480', 'BRL': '34480', 'BRM': '34480', 'BRN': '34480',
    'PCR': '34480', 'PBU': '34480', 'PBR': '34480', 'PUM': '34480',
    'FTI': '34480', 'FTL': '34480', 'PRU': '34480',
    'VAL': '34530', 'DIL': '34530', 'FIL': '34530', 'POS': '34530',
    'PRO': '34530', 'PRE': '34530',
    'IFS': '34810', 'IFD': '34810', 'IFU': '34810', 'IFO': '34810',
    'ILS': '34810', 'ILB': '34810', 'ILU': '34810', 'ILL': '34810',
    'ALC': '34810', 'BFC': '34810', 'BLC': '34810', 'DLC': '34810',
    'RFC': '34810', 'RLC': '34810', 'PLC': '34810', 'BUF': '34810',
    'BRA': '34810', 'BUL': '34810', 'FSI': '34810', 'FUI': '34810',
    'LSI': '34810', 'LUI': '34810', 'UFI': '34810', 'UDI': '34810',
    'LUO': '34810', 'LSO': '34810', 'FUO': '34810', 'FSO': '34810',
    'SFC': '34821',
    'SLC': '34822',
    'BGG': '34831', 'GGI': '34831',
    'BGF': '34832', 'GFI': '34832', 'APG': '34832',
    'BGT': '34840', 'BGP': '34840', 'GPI': '34840', 'GTI': '34840',
    'SGL': '34850', 'SGC': '34850', 'SLI': '34850', 'SCI': '34850',
    '190': '34899', '200': '34899'
}

def map_nsrsliab(code):
    """Maps product code to NSRSLIAB"""
    return NSRSLIAB_MAPPING.get(code, '99999')

# Example usage with pandas DataFrame
def apply_format_mappings(df, product_column='product_code', days_column='days'):
    """
    Apply all format mappings to a DataFrame
    
    Parameters:
    -----------
    df : pandas DataFrame
        DataFrame containing product codes and days
    product_column : str
        Name of column containing product codes
    days_column : str
        Name of column containing days for DAYR mapping
    
    Returns:
    --------
    DataFrame with additional columns for each mapping
    """
    df = df.copy()
    
    # Apply DAYR mapping
    df['DAYR'] = dayr_vectorized(df[days_column])
    
    # Apply $DIRCT mapping
    df['DIRCT'] = df[product_column].map(map_direct)
    
    # Apply $LIAB mapping
    df['LIAB'] = df[product_column].map(map_liab)
    
    # Apply $BTFCEPT mapping
    df['BTFCEPT'] = df[product_column].map(map_btfcept)
    
    # Apply $PRCTYPE mapping
    df['PRCTYPE'] = df[product_column].map(map_prctype)
    
    # Apply $PRCTYPESFS mapping
    df['PRCTYPESFS'] = df[product_column].map(map_prctypesfs)
    
    # Apply $NSRSLIAB mapping
    df['NSRSLIAB'] = df[product_column].map(map_nsrsliab)
    
    return df
# 
# result_df = apply_format_mappings(sample_df)
# print(result_df)
