"""
/*****************************************************************/
/* TAKE NOTE : ANY AMENDMENTS TO PRODUCTS MAPPING, THESE FORMATS */
/*             APPLY TO BANK TRADE DATASET                       */
/* CREATE    : 28 JUNE 2007 (TGX)                                */
/*****************************************************************/
"""

def dayr_format(value):
    """INVALUE DAYR - Converts day number to a category"""
    if value is None:
        return None
    
    try:
        value = float(value)
    except (ValueError, TypeError):
        return None
    
    if value <= 30:
        return 0
    elif 31 <= value <= 59:
        return 1
    elif 60 <= value <= 89:
        return 2
    elif 90 <= value <= 121:
        return 3
    elif 122 <= value <= 151:
        return 4
    elif 152 <= value <= 182:
        return 5
    elif 183 <= value <= 213:
        return 6
    elif 214 <= value <= 243:
        return 7
    elif 244 <= value <= 273:
        return 8
    elif 274 <= value <= 303:
        return 9
    elif 304 <= value <= 333:
        return 10
    elif 334 <= value <= 364:
        return 11
    elif 365 <= value <= 394:
        return 12
    elif 395 <= value <= 424:
        return 13
    elif 425 <= value <= 456:
        return 14
    elif 457 <= value <= 486:
        return 15
    elif 487 <= value <= 516:
        return 16
    elif 517 <= value <= 547:
        return 17
    elif 548 <= value <= 577:
        return 18
    elif 578 <= value <= 608:
        return 19
    elif 609 <= value <= 638:
        return 20
    elif 639 <= value <= 668:
        return 21
    elif 669 <= value <= 698:
        return 22
    elif 699 <= value <= 729:
        return 23
    else:  # value >= 730
        return 24


def dirct_format(value):
    """VALUE $DIRCT - Direction format"""
    if value is None:
        return ' '
    
    value = str(value).strip().upper()
    
    d_codes = {
        'MDL', 'MLL', 'MOL', 'MOF', 'MCL', 'MCF', 'PEF', 'PCE', 'PCB',
        'TRX', 'TRL', 'TRC', 'BAX', 'BAL', 'FBP', 'FBD', 'FBC', 'IEF',
        'TFL', 'TML', 'TFC', 'TMC', 'TFO', 'TMO', 'TLL', 'TNL', 'TLC', 'TNC',
        'TNO', 'PBU', 'PBR', 'TLZ', 'TLQ', 'TLS', 'TLX', 'TLY', 'TLO',
        'BAP', 'BAI', 'BAS', 'BAE', 'PBA', 'FRL', 'TRF',
        'FAS', 'FAU', 'PFU', 'FDS', 'FDU', 'PFD', 'FCL', 'FTB', 'FTL', 'FTI',
        'DAS', 'DAU', 'PAU', 'DDS', 'DDU', 'DDT', 'PDU', 'PDT', 'ITB', 'PTB',
        'POS', 'PRO', 'BRM', 'BRN', 'PBO', 'PBD', 'PBQ', 'PBZ', 'PFT',
        'VAL', 'DIL', 'FIL', 'PRE', 'PCR', 'BRF', 'BRL', 'PUM',
        'TFI', 'TBI', 'TLI', 'TXI', 'BPI', 'BII', 'BSI', 'BEI',
        'PTR', 'PRU', 'PBI', 'PCP', 'MFL'
    }
    
    i_codes = {
        'IFS', 'IFD', 'IFU', 'IFO', 'ILS', 'ILB', 'ILU', 'ILL', 'SFC', 'SLC',
        'TFR', 'TLR', 'BFC', 'BLC', 'DLC', 'RFC', 'RLC', 'PLC', 'ALC',
        'SGL', 'SGC', 'APG', 'BGF', 'BGT', 'BGP',
        '190', '200', 'BUF', 'BUL', 'BRA',
        'FSI', 'FUI', 'LSI', 'LUI', 'SLI', 'SCI', 'GTI', 'GPI',
        'GFI', 'UFI', 'UDI', 'BGG', 'GGI'
    }
    
    if value in d_codes:
        return 'D'
    elif value in i_codes:
        return 'I'
    else:
        return ' '


def liab_format(value):
    """VALUE $LIAB - Liability format"""
    if value is None:
        return '99999'
    
    value = str(value).strip().upper()
    
    liab_mappings = {
        '34810': {
            'IFS', 'IFD', 'IFU', 'IFO', 'ILS', 'ILB', 'ILU', 'ILL',
            'ALC', 'TFR', 'BLC', 'DLC', 'RLC', 'PLC', 'BUF', 'UFI',
            'FSI', 'FUI', 'LSI', 'LSU', 'IUF', 'IUL', 'UDI',
            'BUL', 'BRA', '190', '200', 'BFC', 'RFC', 'LUI',
            'LUO', 'LSO', 'FUO', 'FSO'
        },
        '34821': {'SFC'},
        '34822': {'SLC'},
        '34480': {
            'BRF', 'BRM', 'BRL', 'BRN', 'PBU', 'PBR', 'PUM', 'PFT',
            'PCR', 'PEF', 'PCE', 'PCP', 'FRL', 'TRF', 'PRU',
            'FTL', 'FTI'
        },
        '34440': {
            'TFL', 'TML', 'TFC', 'TMC', 'TFO', 'TMO', 'TLL',
            'TNL', 'TLC', 'TNC', 'TLO', 'TNO',
            'TLF',
            'TLZ', 'TLQ', 'TLS', 'TLX', 'TLY', 'TLW', 'TLV',
            'TFI', 'TBI', 'TLI', 'TXI', 'PTR'
        },
        '34422': {
            'FAS', 'FAU', 'FDS', 'FDU', 'FCL', 'FTB', 'FFS',
            'FFU', 'FCS', 'FCU', 'FFL'
        },
        '34421': {
            'DAS', 'DAU', 'DDS', 'DDU', 'DDT', 'ITB', 'PAU',
            'PDU', 'PDT', 'PTB', 'PFU', 'PFD'
        },
        '34470': {
            'BAP', 'BAI', 'BAS', 'BAE', 'PBA', 'PBO', 'PBZ',
            'PBD', 'PBQ', 'BPI', 'BII', 'BSI', 'BEI', 'PBI'
        },
        '34411': {
            'MDL', 'MOL', 'MLL', 'MFL',
            'VAL', 'DIL', 'FIL', 'PRE', 'MOF'
        },
        '34412': {'POS', 'PRO'},
        '34850': {'SGL', 'SGC', 'ISL', 'ISC', 'SLI', 'SCI'},
        '34840': {'BGT', 'BGP', 'GTI', 'GPI'},
        '34831': {'BGG', 'GGI'},
        '34832': {'BGF', 'APG', 'GFI'},
        '34490': {'MCF', 'IEF'}
    }
    
    for result_code, codes in liab_mappings.items():
        if value in codes:
            return result_code
    
    return '99999'


def btfcept_format(value):
    """VALUE $BTFCEPT - BTF concept format"""
    if value is None:
        return '99'
    
    value = str(value).strip().upper()
    
    btfcept_mappings = {
        '12': {'TBI', 'TXI'},
        '19': {'BSI'},
        '22': {
            'FSI', 'FUI', 'LSI', 'LUI', 'UFI', 'UDI',
            'LUO', 'LSO', 'FUO', 'FSO'
        },
        '23': {'SLI', 'SCI', 'GFI', 'GPI', 'GTI', 'GGI'},
        '35': {'BPI'},
        '36': {'TFI', 'TLI', 'PRU'},
        '49': {'BII', 'BEI', 'PBI'}
    }
    
    for result_code, codes in btfcept_mappings.items():
        if value in codes:
            return result_code
    
    return '99'


def prctype_format(value):
    """VALUE $PRCTYPE - Type of pricing format"""
    if value is None:
        return '99'
    
    value = str(value).strip().upper()
    
    prctype_mappings = {
        '00': {
            'FSI', 'FUI', 'LSI', 'LUI', 'FSO', 'FUO', 'LSO', 'LUO',
            'UFI', 'UDI', 'GTI', 'GPI', 'GFI', 'SLI', 'SCI', 'ALC',
            'IFD', 'IFO', 'ILS', 'ILU', 'IFS', 'IFU', 'ILB', 'ILL',
            'SFC', 'SLC', 'BFC', 'BLC', 'DLC', 'RFC', 'RLC', 'FAS',
            'FAU', 'FDS', 'FDU', 'FCL', 'FFS', 'FFU', 'FCS', 'FCU',
            'FFL', 'FTB', 'BUF', 'BUL', 'BGT', 'BGP', 'BGF', 'APG',
            'SGL', 'SGC', 'BGG', 'GGI'
        },
        '41': {
            'TFI', 'TLI', 'TFL', 'TLL', 'TLZ', 'DAS',
            'DAU', 'DDS', 'DDU', 'ITB', 'BRF', 'BRL', 'PBU', 'PDU',
            'PRE', 'PRO', 'PBA', 'PBZ'
        },
        '53': {
            'BPI', 'BSI', 'BII', 'BEI', 'BAS', 'BAE', 'BAP', 'BAI'
        },
        '68': {'FTI', 'FTL'},
        '79': {'PBI', 'PRU', 'VAL', 'DIL', 'FIL', 'POS'}
    }
    
    for result_code, codes in prctype_mappings.items():
        if value in codes:
            return result_code
    
    return '99'


def prctypesfs_format(value):
    """VALUE $PRCTYPESFS - Type of pricing - SFS format"""
    if value is None:
        return '99'
    
    value = str(value).strip().upper()
    
    prctypesfs_mappings = {
        '00': {
            'FSI', 'FUI', 'LSI', 'LUI', 'FSO', 'FUO', 'LSO', 'LUO',
            'UFI', 'UDI', 'GTI', 'GPI', 'GFI', 'SLI', 'SCI', 'ALC',
            'IFD', 'IFO', 'ILS', 'ILU', 'IFS', 'IFU', 'ILB', 'ILL',
            'SFC', 'SLC', 'BFC', 'BLC', 'DLC', 'RFC', 'RLC', 'FAS',
            'FAU', 'FDS', 'FDU', 'FCL', 'FFS', 'FFU', 'FCS', 'FCU',
            'FFL', 'FTB', 'BUF', 'BUL', 'BGT', 'BGP', 'BGF', 'APG',
            'SGL', 'SGC', 'BGG', 'GGI'
        },
        '41': {
            'DAS', 'DAU', 'DDS', 'DDU', 'ITB', 'PBU', 'PDU', 'PRE', 'PRO'
        },
        '68': {'FTI', 'FTL'},
        '79': {'PBI', 'PRU', 'VAL', 'DIL', 'FIL', 'POS'},
        '59': {
            'TFI', 'TLI', 'BPI', 'BSI', 'BII', 'BEI', 'TFL', 'TLL',
            'TLZ', 'BRF', 'BRL', 'PBA', 'PBZ', 'BAS', 'BAE', 'BAP',
            'BAI'
        }
    }
    
    for result_code, codes in prctypesfs_mappings.items():
        if value in codes:
            return result_code
    
    return '99'


def nsrsliab_format(value):
    """VALUE $NSRSLIAB - NSRS Liability format"""
    if value is None:
        return '99999'
    
    value = str(value).strip().upper()
    
    nsrsliab_mappings = {
        '34440': {
            'TFL', 'TML', 'TFC', 'TMC', 'TFO', 'TMO', 'TLL',
            'TNL', 'TLC', 'TNC', 'TLO', 'TNO', 'TLF', 'TLQ',
            'TLS', 'TLV', 'TLW', 'TLX', 'TLY', 'TLZ', 'TRF',
            'TFI', 'TBI', 'TLI', 'TXI'
        },
        '34473': {'BAE', 'BEI'},
        '34474': {'BAI', 'BII'},
        '34476': {
            'BAP', 'BAS', 'PBA', 'PBO', 'PBD', 'PBQ', 'PBZ',
            'BPI', 'BSI', 'PBI'
        },
        '34479': {
            'FAS', 'FAU', 'FDS', 'FDU', 'FCL', 'FTB', 'DAS',
            'DAU', 'DDS', 'DDU', 'DDT', 'ITB', 'IEF', 'MDL',
            'MOL', 'MLL', 'MFL', 'MOF', 'MCF', 'PDU', 'PCE',
            'PCP', 'PEF', 'PFD', 'PFU', 'PTB', 'FFS', 'FFU',
            'FCS', 'FCU', 'FFL'
        },
        '34480': {
            'BRF', 'BRL', 'BRM', 'BRN', 'PCR', 'PBU', 'PBR',
            'PUM', 'FTI', 'FTL', 'PRU'
        },
        '34530': {'VAL', 'DIL', 'FIL', 'POS', 'PRO', 'PRE'},
        '34810': {
            'IFS', 'IFD', 'IFU', 'IFO', 'ILS', 'ILB', 'ILU',
            'ILL', 'ALC', 'BFC', 'BLC', 'DLC', 'RFC', 'RLC',
            'PLC', 'BUF', 'BRA', 'BUL', 'FSI', 'FUI', 'LSI',
            'LUI', 'UFI', 'UDI', 'LUO', 'LSO', 'FUO', 'FSO'
        },
        '34821': {'SFC'},
        '34822': {'SLC'},
        '34831': {'BGG', 'GGI'},
        '34832': {'BGF', 'GFI', 'APG'},
        '34840': {'BGT', 'BGP', 'GPI', 'GTI'},
        '34850': {'SGL', 'SGC', 'SLI', 'SCI'},
        '34899': {'190', '200'}
    }
    
    for result_code, codes in nsrsliab_mappings.items():
        if value in codes:
            return result_code
    
    return '99999'
