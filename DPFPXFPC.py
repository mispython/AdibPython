def parse_rpvdata() -> pl.DataFrame:
    """Exact copy of your original working parsing logic"""
    records = []
    with open(RPVBDATA_PATH, 'r') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith('0'):
            continue
        if line.startswith('1'):
            parts = line.split()
            if len(parts) >= 15:
                record = {
                    'MNIACTNO': parts[1] if len(parts) > 1 else '',
                    'BRANCHNO': parts[2] if len(parts) > 2 else '',
                    'NAME': ' '.join(parts[3:8]) if len(parts) > 8 else parts[3] if len(parts) > 3 else '',
                    'ACCTSTA': parts[8] if len(parts) > 8 else '',
                    'PRSTCOND': parts[9] if len(parts) > 9 else '',
                    'REGCARD': parts[10] if len(parts) > 10 else '',
                    'IGNTKEY': parts[11] if len(parts) > 11 else '',
                    'ACCTWOFF': parts[12] if len(parts) > 12 else '',
                    'MODEREPO': parts[13] if len(parts) > 13 else '',
                    'REPOSTAT': parts[14] if len(parts) > 14 else '',
                    'MODEDISP': parts[15] if len(parts) > 15 else '',
                    'YY1': parts[16][:4] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                    'MM1': parts[16][4:6] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                    'DD1': parts[16][6:8] if len(parts) > 16 and len(parts[16]) >= 8 else None,
                }
                records.append(record)
    
    logger.info(f"Parsed {len(records)} records from {RPVBDATA_PATH}")
    return pl.DataFrame(records)
  
