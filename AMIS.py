LRECL = 80

LAYOUT = {'BRCODE': {'type': 'packed', 'slice': slice(1, 9, None)},
 'BRABBR': {'type': 'ebcdic', 'slice': slice(5, 8, None)},
 'BRSTAT': {'type': 'ebcdic', 'slice': slice(11, 40, None)},
 'BRSTATEIND': {'type': 'ebcdic', 'slice': slice(44, 45, None)},
 'BRSTATUS': {'type': 'ebcdic', 'slice': slice(49, 50, None)}}

/sasdata/rawdata/lookup/LKP_BRANCH
