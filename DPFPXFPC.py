# =============================================================================
# IMPORTS FROM EXISTING MODULES
# =============================================================================
try:
    from PBBELF import (CTYPE_MAP, lcrcdmni_fmt, lcrcdmniopr_fmt, lcrcdgl_fmt, 
                        lcrcdglccy_fmt, lcrcdgloth_fmt, lcrcdequ_fmt, colid_fmt)
except ImportError:
    print("Warning: PBBELF.py not found, using default mappings")
    CTYPE_MAP = {}
    def lcrcdmni_fmt(code): return code or ''
    def lcrcdmniopr_fmt(code): return code or ''
    def lcrcdgl_fmt(code): return code or ''
    def lcrcdglccy_fmt(code): return code or ''
    def lcrcdgloth_fmt(code): return code or ''
    def lcrcdequ_fmt(code): return code or ''
    def colid_fmt(code): return code or ''

try:
    from PBLCRFMT import remfmt, cmmfmt, remfmx
except ImportError:
    print("Warning: PBLCRFMT.py not found, using default format functions")
    def remfmt(value):
        if value is None: return '06'
        if value <= 1: return '01'
        if value <= 3: return '02'
        if value <= 6: return '03'
        if value <= 9: return '04'
        if value <= 12: return '05'
        return '06'
    
    def cmmfmt(value):
        if value is None: return '06'
        if value <= 0.1: return '01'
        if value <= 1: return '02'
        if value <= 3: return '03'
        if value <= 6: return '04'
        if value <= 12: return '05'
        return '06'
    
    def remfmx(value):
        if value is None: return '03'
        if value < 6: return '01'
        if value < 12: return '02'
        return '03'
