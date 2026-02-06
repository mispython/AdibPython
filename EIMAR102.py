import polars as pl

BNM_REPTDATE = 'data/bnm/reptdate.parquet'
BNM_LOANTEMP = 'data/bnm/loantemp.parquet'
BRHFILE = 'data/brhfile.parquet'
CCDTXT2 = 'data/ccdtxt2_eimar102.txt'

HPD_PRODUCTS = [500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510]

df_reptdate = pl.read_parquet(BNM_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

rdate = reptdate.strftime('%d%m%y')
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')

df_brhdata = pl.read_parquet(BRHFILE).select(['BRANCH', 'BRABBR']).rename({'BRABBR': 'BRHCODE'})

records = []

for row in pl.read_parquet(BNM_LOANTEMP).filter(
    (pl.col('BALANCE') > 0) & (pl.col('BORSTAT') != 'Z')
).iter_rows(named=True):
    if row['PRODUCT'] in [380, 381, 700, 705, 720, 725]:
        records.append({**row, 'CAT': 'A', 'TYPE': '(HPD-C)'})
    
    if row['PRODUCT'] in [380, 381]:
        records.append({**row, 'CAT': 'B', 'TYPE': '(HP 380/381)'})
    
    if row['PRODUCT'] in [128, 130, 131, 132]:
        records.append({**row, 'CAT': 'C', 'TYPE': '(AITAB)'})
    
    if row['PRODUCT'] in HPD_PRODUCTS:
        records.append({**row, 'CAT': 'D', 'TYPE': '(-HPD-)'})

df_loantemp = pl.DataFrame(records).sort('BRANCH').join(
    df_brhdata, on='BRANCH', how='left'
)

if reptday != '15':
    df_loantemp = df_loantemp.sort(['CAT', 'BRANCH', 'ARREAR'])
    num_buckets = 17
    arrear_field = 'ARREAR'
    progid = 'EIMAR102-A'
    
    def write_report_17():
        lines = []
        page_count = 0
        current_cat = None
        
        totamt = [0] * num_buckets
        totacc = [0] * num_buckets
        
        for row in df_loantemp.iter_rows(named=True):
            if current_cat != row['CAT']:
                if current_cat is not None:
                    sgtotbrh = sum(totamt[3:17])
                    sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
                    sgtotacc = sum(totacc[3:17])
                    sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
                    gtotbrh = sum(totamt[0:3]) + sgtotbrh
                    gtotacc = sum(totacc[0:3]) + sgtotacc
                    
                    lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                    lines.append(f"TOT {totacc[0]:>7,} {totamt[0]:>16,.2f} {totacc[1]:>7,} {totamt[1]:>15,.2f} {totacc[2]:>7,} {totamt[2]:>15,.2f} {totacc[3]:>8,} {totamt[3]:>17,.2f} {totacc[4]:>8,} {totamt[4]:>17,.2f}")
                    lines.append(f"    {totacc[5]:>7,} {totamt[5]:>16,.2f} {totacc[6]:>7,} {totamt[6]:>15,.2f} {totacc[7]:>7,} {totamt[7]:>15,.2f} {totacc[8]:>8,} {totamt[8]:>17,.2f} {totacc[9]:>8,} {totamt[9]:>17,.2f}")
                    lines.append(f"    {totacc[10]:>7,} {totamt[10]:>16,.2f} {totacc[11]:>7,} {totamt[11]:>15,.2f} {totacc[12]:>7,} {totamt[12]:>15,.2f} {totacc[13]:>8,} {totamt[13]:>17,.2f} {totacc[14]:>8,} {totamt[14]:>17,.2f}")
                    lines.append(f"    {totacc[15]:>7,} {totamt[15]:>16,.2f} {totacc[16]:>7,} {totamt[16]:>15,.2f} {sgtotacc:>7,} {sgtotbrh:>15,.2f} {sgtotac2:>8,} {sgtotbr2:>17,.2f} {gtotacc:>8,} {gtotbrh:>17,.2f}")
                    lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                    lines.append('')
                
                current_cat = row['CAT']
                page_count += 1
                totamt = [0] * num_buckets
                totacc = [0] * num_buckets
                
                lines.append(f"PROGRAM-ID : {progid}                  P U B L I C   B A N K   B E R H A D                                             PAGE NO.: {page_count}")
                lines.append(f"                                     OUTSTANDING LOANS IN ARREARS {row['TYPE']:13} {rdate}")
                lines.append(' ')
                lines.append('BRH    NO          < 1 MTH              NO     1 TO < 2 MTH          NO     2 TO < 3 MTH         NO      3 TO < 4 MTH          NO      4 TO < 5 MTH')
                lines.append('       NO     5 TO < 6 MTH              NO     6 TO < 7 MTH          NO     7 TO < 8 MTH         NO      8 TO < 9 MTH          NO     9 TO < 10 MTH')
                lines.append('       NO   10 TO < 11 MTH              NO   11 TO < 12 MTH          NO   12 TO < 18 MTH         NO    18 TO < 24 MTH          NO    24 TO < 36 MTH')
                lines.append('       NO         > 36 MTH              NO          DEFICIT          NO   SUBTOTAL >=3MTH         NO   SUBTOTAL >=6MTH          NO             TOTAL')
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
            
            current_branch = row['BRANCH']
            brhamt = [0] * num_buckets
            noacc = [0] * num_buckets
            
            df_branch = df_loantemp.filter(
                (pl.col('CAT') == row['CAT']) & (pl.col('BRANCH') == row['BRANCH'])
            )
            
            for branch_row in df_branch.iter_rows(named=True):
                if branch_row['BALANCE'] > 0:
                    arrear_idx = branch_row[arrear_field] - 1
                    if 0 <= arrear_idx < num_buckets:
                        brhamt[arrear_idx] += branch_row['BALANCE']
                        noacc[arrear_idx] += 1
            
            for i in range(num_buckets):
                totamt[i] += brhamt[i]
                totacc[i] += noacc[i]
            
            subbrh = sum(brhamt[3:17])
            subbr2 = subbrh - brhamt[3] - brhamt[4] - brhamt[5]
            subacc = sum(noacc[3:17])
            subac2 = subacc - noacc[3] - noacc[4] - noacc[5]
            totbrh = sum(brhamt[0:3]) + subbrh
            sotacc = sum(noacc[0:3]) + subacc
            
            lines.append(f"{row['BRANCH']:03d} {noacc[0]:>7,} {brhamt[0]:>16,.2f} {noacc[1]:>7,} {brhamt[1]:>15,.2f} {noacc[2]:>7,} {brhamt[2]:>15,.2f} {noacc[3]:>8,} {brhamt[3]:>17,.2f} {noacc[4]:>8,} {brhamt[4]:>17,.2f}")
            lines.append(f"{row['BRHCODE']:3} {noacc[5]:>7,} {brhamt[5]:>16,.2f} {noacc[6]:>7,} {brhamt[6]:>15,.2f} {noacc[7]:>7,} {brhamt[7]:>15,.2f} {noacc[8]:>8,} {brhamt[8]:>17,.2f} {noacc[9]:>8,} {brhamt[9]:>17,.2f}")
            lines.append(f"    {noacc[10]:>7,} {brhamt[10]:>16,.2f} {noacc[11]:>7,} {brhamt[11]:>15,.2f} {noacc[12]:>7,} {brhamt[12]:>15,.2f} {noacc[13]:>8,} {brhamt[13]:>17,.2f} {noacc[14]:>8,} {brhamt[14]:>17,.2f}")
            lines.append(f"    {noacc[15]:>7,} {brhamt[15]:>16,.2f} {noacc[16]:>7,} {brhamt[16]:>15,.2f} {subacc:>7,} {subbrh:>15,.2f} {subac2:>8,} {subbr2:>17,.2f} {sotacc:>8,} {totbrh:>17,.2f}")
            
            df_loantemp = df_loantemp.filter(
                ~((pl.col('CAT') == row['CAT']) & (pl.col('BRANCH') == row['BRANCH']))
            )
        
        if current_cat is not None:
            sgtotbrh = sum(totamt[3:17])
            sgtotbr2 = sgtotbrh - totamt[3] - totamt[4] - totamt[5]
            sgtotacc = sum(totacc[3:17])
            sgtotac2 = sgtotacc - totacc[3] - totacc[4] - totacc[5]
            gtotbrh = sum(totamt[0:3]) + sgtotbrh
            gtotacc = sum(totacc[0:3]) + sgtotacc
            
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
            lines.append(f"TOT {totacc[0]:>7,} {totamt[0]:>16,.2f} {totacc[1]:>7,} {totamt[1]:>15,.2f} {totacc[2]:>7,} {totamt[2]:>15,.2f} {totacc[3]:>8,} {totamt[3]:>17,.2f} {totacc[4]:>8,} {totamt[4]:>17,.2f}")
            lines.append(f"    {totacc[5]:>7,} {totamt[5]:>16,.2f} {totacc[6]:>7,} {totamt[6]:>15,.2f} {totacc[7]:>7,} {totamt[7]:>15,.2f} {totacc[8]:>8,} {totamt[8]:>17,.2f} {totacc[9]:>8,} {totamt[9]:>17,.2f}")
            lines.append(f"    {totacc[10]:>7,} {totamt[10]:>16,.2f} {totacc[11]:>7,} {totamt[11]:>15,.2f} {totacc[12]:>7,} {totamt[12]:>15,.2f} {totacc[13]:>8,} {totamt[13]:>17,.2f} {totacc[14]:>8,} {totamt[14]:>17,.2f}")
            lines.append(f"    {totacc[15]:>7,} {totamt[15]:>16,.2f} {totacc[16]:>7,} {totamt[16]:>15,.2f} {sgtotacc:>7,} {sgtotbrh:>15,.2f} {sgtotac2:>8,} {sgtotbr2:>17,.2f} {gtotacc:>8,} {gtotbrh:>17,.2f}")
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        
        with open(CCDTXT2, 'w') as f:
            for line in lines:
                f.write(line + '\n')
    
    write_report_17()

else:
    df_loantemp = df_loantemp.sort(['CAT', 'BRANCH', 'ARREAR2'])
    num_buckets = 15
    arrear_field = 'ARREAR2'
    progid = 'EIMAR102-B'
    
    def write_report_15():
        lines = []
        page_count = 0
        current_cat = None
        
        totamt = [0] * num_buckets
        totacc = [0] * num_buckets
        
        for row in df_loantemp.iter_rows(named=True):
            if current_cat != row['CAT']:
                if current_cat is not None:
                    sgtotbrh = sum(totamt[3:15])
                    sgtotbr2 = sum(totamt[6:15])
                    sgtotacc = sum(totacc[3:15])
                    sgtotac2 = sum(totacc[6:15])
                    gtotbrh = sum(totamt[0:3]) + sgtotbrh
                    gtotacc = sum(totacc[0:3]) + sgtotacc
                    
                    lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                    lines.append(f"TOT {totacc[0]:>7,} {totamt[0]:>16,.2f} {totacc[1]:>7,} {totamt[1]:>15,.2f} {totacc[2]:>7,} {totamt[2]:>15,.2f} {totacc[3]:>8,} {totamt[3]:>17,.2f} {totacc[4]:>8,} {totamt[4]:>17,.2f}")
                    lines.append(f"    {totacc[5]:>7,} {totamt[5]:>16,.2f} {totacc[6]:>7,} {totamt[6]:>15,.2f} {totacc[7]:>7,} {totamt[7]:>15,.2f} {totacc[8]:>8,} {totamt[8]:>17,.2f} {totacc[9]:>8,} {totamt[9]:>17,.2f}")
                    lines.append(f"    {totacc[10]:>7,} {totamt[10]:>16,.2f} {totacc[11]:>7,} {totamt[11]:>15,.2f} {totacc[12]:>7,} {totamt[12]:>15,.2f} {totacc[13]:>8,} {totamt[13]:>17,.2f} {totacc[14]:>8,} {totamt[14]:>17,.2f}")
                    lines.append(f"                                                      {sgtotacc:>7,} {sgtotbrh:>15,.2f} {sgtotac2:>8,} {sgtotbr2:>17,.2f} {gtotacc:>8,} {gtotbrh:>17,.2f}")
                    lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                    lines.append('')
                
                current_cat = row['CAT']
                page_count += 1
                totamt = [0] * num_buckets
                totacc = [0] * num_buckets
                
                lines.append(f"PROGRAM-ID : {progid}                  P U B L I C   B A N K   B E R H A D                                             PAGE NO.: {page_count}")
                lines.append(f"                                         OUTSTANDING LOANS IN ARREARS {row['TYPE']:13} {rdate}")
                lines.append(' ')
                lines.append('BRH    NO          < 1 MTH              NO     1 TO < 2 MTH          NO     2 TO < 3 MTH         NO      3 TO < 4 MTH          NO      4 TO < 5 MTH')
                lines.append('       NO     5 TO < 6 MTH              NO     6 TO < 7 MTH          NO     7 TO < 8 MTH         NO      8 TO < 9 MTH          NO     9 TO < 12 MTH')
                lines.append('       NO   12 TO < 18 MTH              NO   18 TO < 24 MTH          NO   24 TO < 36 MTH         NO          > 36 MTH          NO           DEFICIT')
                lines.append('                                                                      NO   SUBTOTAL >=3MTH         NO   SUBTOTAL >=6MTH          NO             TOTAL')
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
            
            current_branch = row['BRANCH']
            brhamt = [0] * num_buckets
            noacc = [0] * num_buckets
            
            df_branch = df_loantemp.filter(
                (pl.col('CAT') == row['CAT']) & (pl.col('BRANCH') == row['BRANCH'])
            )
            
            for branch_row in df_branch.iter_rows(named=True):
                if branch_row['BALANCE'] > 0:
                    arrear_idx = branch_row[arrear_field] - 1
                    if 0 <= arrear_idx < num_buckets:
                        brhamt[arrear_idx] += branch_row['BALANCE']
                        noacc[arrear_idx] += 1
            
            for i in range(num_buckets):
                totamt[i] += brhamt[i]
                totacc[i] += noacc[i]
            
            subbrh = sum(brhamt[3:15])
            subbr2 = sum(brhamt[6:15])
            subacc = sum(noacc[3:15])
            subac2 = sum(noacc[6:15])
            totbrh = sum(brhamt[0:3]) + subbrh
            sotacc = sum(noacc[0:3]) + subacc
            
            lines.append(f"{row['BRANCH']:03d} {noacc[0]:>7,} {brhamt[0]:>16,.2f} {noacc[1]:>7,} {brhamt[1]:>15,.2f} {noacc[2]:>7,} {brhamt[2]:>15,.2f} {noacc[3]:>8,} {brhamt[3]:>17,.2f} {noacc[4]:>8,} {brhamt[4]:>17,.2f}")
            lines.append(f"{row['BRHCODE']:3} {noacc[5]:>7,} {brhamt[5]:>16,.2f} {noacc[6]:>7,} {brhamt[6]:>15,.2f} {noacc[7]:>7,} {brhamt[7]:>15,.2f} {noacc[8]:>8,} {brhamt[8]:>17,.2f} {noacc[9]:>8,} {brhamt[9]:>17,.2f}")
            lines.append(f"    {noacc[10]:>7,} {brhamt[10]:>16,.2f} {noacc[11]:>7,} {brhamt[11]:>15,.2f} {noacc[12]:>7,} {brhamt[12]:>15,.2f} {noacc[13]:>8,} {brhamt[13]:>17,.2f} {noacc[14]:>8,} {brhamt[14]:>17,.2f}")
            lines.append(f"                                                      {subacc:>7,} {subbrh:>15,.2f} {subac2:>8,} {subbr2:>17,.2f} {sotacc:>8,} {totbrh:>17,.2f}")
            
            df_loantemp = df_loantemp.filter(
                ~((pl.col('CAT') == row['CAT']) & (pl.col('BRANCH') == row['BRANCH']))
            )
        
        if current_cat is not None:
            sgtotbrh = sum(totamt[3:15])
            sgtotbr2 = sum(totamt[6:15])
            sgtotacc = sum(totacc[3:15])
            sgtotac2 = sum(totacc[6:15])
            gtotbrh = sum(totamt[0:3]) + sgtotbrh
            gtotacc = sum(totacc[0:3]) + sgtotacc
            
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
            lines.append(f"TOT {totacc[0]:>7,} {totamt[0]:>16,.2f} {totacc[1]:>7,} {totamt[1]:>15,.2f} {totacc[2]:>7,} {totamt[2]:>15,.2f} {totacc[3]:>8,} {totamt[3]:>17,.2f} {totacc[4]:>8,} {totamt[4]:>17,.2f}")
            lines.append(f"    {totacc[5]:>7,} {totamt[5]:>16,.2f} {totacc[6]:>7,} {totamt[6]:>15,.2f} {totacc[7]:>7,} {totamt[7]:>15,.2f} {totacc[8]:>8,} {totamt[8]:>17,.2f} {totacc[9]:>8,} {totamt[9]:>17,.2f}")
            lines.append(f"    {totacc[10]:>7,} {totamt[10]:>16,.2f} {totacc[11]:>7,} {totamt[11]:>15,.2f} {totacc[12]:>7,} {totamt[12]:>15,.2f} {totacc[13]:>8,} {totamt[13]:>17,.2f} {totacc[14]:>8,} {totamt[14]:>17,.2f}")
            lines.append(f"                                                      {sgtotacc:>7,} {sgtotbrh:>15,.2f} {sgtotac2:>8,} {sgtotbr2:>17,.2f} {gtotacc:>8,} {gtotbrh:>17,.2f}")
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        
        with open(CCDTXT2, 'w') as f:
            for line in lines:
                f.write(line + '\n')
    
    write_report_15()

print(f"Report generated: {CCDTXT2}")
