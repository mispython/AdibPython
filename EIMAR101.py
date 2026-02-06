import polars as pl

BNM_REPTDATE = 'data/bnm/reptdate.parquet'
BNM_LOANTEMP = 'data/bnm/loantemp.parquet'
BRHFILE = 'data/brhfile.parquet'
CCDTXT2 = 'data/ccdtxt2.txt'
CCDTXT7A = 'data/ccdtxt7a.txt'

HPD_PRODUCTS = [500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510]

df_reptdate = pl.read_parquet(BNM_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

rdate = reptdate.strftime('%d%m%y')
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')

df_loan = pl.read_parquet(BNM_LOANTEMP).sort(['BRANCH', 'ARREAR2'])

df_brhdata = pl.read_parquet(BRHFILE).select(['BRANCH', 'BRABBR']).rename({'BRABBR': 'BRHCODE'})

records = []

for row in df_loan.filter((pl.col('BALANCE') > 0) & (pl.col('BORSTAT') != 'Z')).iter_rows(named=True):
    if row['PRODUCT'] in [380, 381, 700, 705, 720, 725]:
        records.append({**row, 'CAT': 'A', 'TYPE': '(HPD-C)'})
    
    if row['PRODUCT'] in [380, 381]:
        records.append({**row, 'CAT': 'B', 'TYPE': '(HP 380/381)'})
    
    if row['PRODUCT'] in [128, 130, 131, 132]:
        records.append({**row, 'CAT': 'C', 'TYPE': '(AITAB)'})
    
    if row['PRODUCT'] in HPD_PRODUCTS:
        records.append({**row, 'CAT': 'D', 'TYPE': '(-HPD-)'})

df_loantemp = pl.DataFrame(records).sort('BRANCH').join(df_brhdata, on='BRANCH', how='left').sort(['CAT', 'BRANCH', 'ARREAR2'])

def write_arrear_report(df, progid, output_file):
    lines = []
    page_count = 0
    
    current_cat = None
    
    totamt = [0] * 14
    totacc = [0] * 14
    
    for row in df.iter_rows(named=True):
        if current_cat != row['CAT']:
            if current_cat is not None:
                sgtotbrh = sum(totamt[3:14])
                sgtotbr2 = sum(totamt[6:14])
                sgtotacc = sum(totacc[3:14])
                sgtotac2 = sum(totacc[6:14])
                gtotbrh = sum(totamt[0:3]) + sgtotbrh
                gtotacc = sum(totacc[0:3]) + sgtotacc
                
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"TOT {totacc[0]:>7,} {totamt[0]:>16,.2f} {totacc[1]:>7,} {totamt[1]:>15,.2f} {totacc[2]:>7,} {totamt[2]:>15,.2f} {totacc[3]:>8,} {totamt[3]:>17,.2f} {totacc[4]:>8,} {totamt[4]:>17,.2f}")
                lines.append(f"    {totacc[5]:>7,} {totamt[5]:>16,.2f} {totacc[6]:>7,} {totamt[6]:>15,.2f} {totacc[7]:>7,} {totamt[7]:>15,.2f} {totacc[8]:>8,} {totamt[8]:>17,.2f} {totacc[9]:>8,} {totamt[9]:>17,.2f}")
                lines.append(f"    {totacc[10]:>7,} {totamt[10]:>16,.2f} {totacc[11]:>7,} {totamt[11]:>15,.2f} {totacc[12]:>7,} {totamt[12]:>15,.2f} {totacc[13]:>8,} {totamt[13]:>17,.2f} {sgtotacc:>8,} {sgtotbrh:>17,.2f}")
                lines.append(f"                                                                                {sgtotac2:>8,} {sgtotbr2:>17,.2f} {gtotacc:>8,} {gtotbrh:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
            
            current_cat = row['CAT']
            page_count += 1
            totamt = [0] * 14
            totacc = [0] * 14
            
            lines.append(f"PROGRAM-ID : {progid}                  P U B L I C   B A N K   B E R H A D                                             PAGE NO.: {page_count}")
            lines.append(f"                                         OUTSTANDING LOANS IN ARREARS {row['TYPE']:13} {rdate}")
            lines.append(' ')
            lines.append('BRH    NO          < 1 MTH              NO     1 TO < 2 MTH          NO     2 TO < 3 MTH         NO      3 TO < 4 MTH          NO      4 TO < 5 MTH')
            lines.append('       NO     5 TO < 6 MTH              NO     6 TO < 7 MTH          NO     7 TO < 8 MTH         NO      8 TO < 9 MTH          NO     9 TO < 12 MTH')
            lines.append('       NO   12 TO < 18 MTH              NO   18 TO < 24 MTH          NO   24 TO < 36 MTH         NO          > 36 MTH          NO   SUBTOTAL >=3MTH')
            lines.append('                                                                                                  NO   SUBTOTAL >=6MTH          NO             TOTAL')
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        
        current_branch = row['BRANCH']
        brhamt = [0] * 14
        noacc = [0] * 14
        
        df_branch = df.filter((pl.col('CAT') == row['CAT']) & (pl.col('BRANCH') == row['BRANCH']))
        
        for branch_row in df_branch.iter_rows(named=True):
            if branch_row['BALANCE'] > 0:
                arrear_idx = branch_row['ARREAR2'] - 1
                if 0 <= arrear_idx < 14:
                    brhamt[arrear_idx] += branch_row['BALANCE']
                    noacc[arrear_idx] += 1
        
        for i in range(14):
            totamt[i] += brhamt[i]
            totacc[i] += noacc[i]
        
        subbrh = sum(brhamt[3:14])
        subbr2 = sum(brhamt[6:14])
        subacc = sum(noacc[3:14])
        subac2 = sum(noacc[6:14])
        totbrh = sum(brhamt[0:3]) + subbrh
        sotacc = sum(noacc[0:3]) + subacc
        
        lines.append(f"{row['BRANCH']:03d} {noacc[0]:>7,} {brhamt[0]:>16,.2f} {noacc[1]:>7,} {brhamt[1]:>15,.2f} {noacc[2]:>7,} {brhamt[2]:>15,.2f} {noacc[3]:>8,} {brhamt[3]:>17,.2f} {noacc[4]:>8,} {brhamt[4]:>17,.2f}")
        lines.append(f"{row['BRHCODE']:3} {noacc[5]:>7,} {brhamt[5]:>16,.2f} {noacc[6]:>7,} {brhamt[6]:>15,.2f} {noacc[7]:>7,} {brhamt[7]:>15,.2f} {noacc[8]:>8,} {brhamt[8]:>17,.2f} {noacc[9]:>8,} {brhamt[9]:>17,.2f}")
        lines.append(f"    {noacc[10]:>7,} {brhamt[10]:>16,.2f} {noacc[11]:>7,} {brhamt[11]:>15,.2f} {noacc[12]:>7,} {brhamt[12]:>15,.2f} {noacc[13]:>8,} {brhamt[13]:>17,.2f} {subacc:>8,} {subbrh:>17,.2f}")
        lines.append(f"                                                                                {subac2:>8,} {subbr2:>17,.2f} {sotacc:>8,} {totbrh:>17,.2f}")
        
        df = df.filter(~((pl.col('CAT') == row['CAT']) & (pl.col('BRANCH') == row['BRANCH'])))
    
    if current_cat is not None:
        sgtotbrh = sum(totamt[3:14])
        sgtotbr2 = sum(totamt[6:14])
        sgtotacc = sum(totacc[3:14])
        sgtotac2 = sum(totacc[6:14])
        gtotbrh = sum(totamt[0:3]) + sgtotbrh
        gtotacc = sum(totacc[0:3]) + sgtotacc
        
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"TOT {totacc[0]:>7,} {totamt[0]:>16,.2f} {totacc[1]:>7,} {totamt[1]:>15,.2f} {totacc[2]:>7,} {totamt[2]:>15,.2f} {totacc[3]:>8,} {totamt[3]:>17,.2f} {totacc[4]:>8,} {totamt[4]:>17,.2f}")
        lines.append(f"    {totacc[5]:>7,} {totamt[5]:>16,.2f} {totacc[6]:>7,} {totamt[6]:>15,.2f} {totacc[7]:>7,} {totamt[7]:>15,.2f} {totacc[8]:>8,} {totamt[8]:>17,.2f} {totacc[9]:>8,} {totamt[9]:>17,.2f}")
        lines.append(f"    {totacc[10]:>7,} {totamt[10]:>16,.2f} {totacc[11]:>7,} {totamt[11]:>15,.2f} {totacc[12]:>7,} {totamt[12]:>15,.2f} {totacc[13]:>8,} {totamt[13]:>17,.2f} {sgtotacc:>8,} {sgtotbrh:>17,.2f}")
        lines.append(f"                                                                                {sgtotac2:>8,} {sgtotbr2:>17,.2f} {gtotacc:>8,} {gtotbrh:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
    
    with open(output_file, 'w') as f:
        for line in lines:
            f.write(line + '\n')

write_arrear_report(df_loantemp, 'EIMAR101-A', CCDTXT2)

df_prndata_b = df_loantemp.filter(
    ~pl.col('TYPE').is_in(['(HPD-C)', '(-HPD-)']) |
    (
        (~pl.col('BORSTAT').is_in(['F', 'I', 'R'])) &
        pl.col('PRODUCT').is_in(HPD_PRODUCTS)
    )
)

write_arrear_report(df_prndata_b, 'EIMAR101-B', CCDTXT2 + '.b')

branch_summary = []

for cat_val in df_loantemp['CAT'].unique():
    df_cat = df_loantemp.filter(pl.col('CAT') == cat_val)
    
    for branch_val in df_cat['BRANCH'].unique():
        df_branch = df_cat.filter(pl.col('BRANCH') == branch_val)
        
        brhamt = [0] * 14
        noacc = [0] * 14
        
        for row in df_branch.iter_rows(named=True):
            if row['BALANCE'] > 0:
                arrear_idx = row['ARREAR2'] - 1
                if 0 <= arrear_idx < 14:
                    brhamt[arrear_idx] += row['BALANCE']
                    noacc[arrear_idx] += 1
        
        brhcode = df_branch['BRHCODE'][0]
        type_val = df_branch['TYPE'][0]
        
        branch_summary.append({
            'BRHCODE': brhcode,
            'TYPE': type_val,
            **{f'NOACC{i+1}': noacc[i] for i in range(14)},
            **{f'BRHAMT{i+1}': brhamt[i] for i in range(14)}
        })

df_loan7a = pl.DataFrame(branch_summary)

with open(CCDTXT7A, 'w') as f:
    f.write('BRHCODE;TYPE;NOACC1;BRHAMT1;NOACC2;BRHAMT2;NOACC3;BRHAMT3;NOACC4;BRHAMT4;')
    f.write('NOACC5;BRHAMT5;NOACC6;BRHAMT6;NOACC7;BRHAMT7;NOACC8;BRHAMT8;NOACC9;BRHAMT9;')
    f.write('NOACC10;BRHAMT10;NOACC11;BRHAMT11;NOACC12;BRHAMT12;NOACC13;BRHAMT13;NOACC14;BRHAMT14;\n')
    
    for row in df_loan7a.iter_rows(named=True):
        f.write(f"{row['BRHCODE']};{row['TYPE']};")
        for i in range(1, 15):
            f.write(f"{row[f'NOACC{i}']};{row[f'BRHAMT{i}']:.2f};")
        f.write('\n')

print(f"Reports generated:")
print(f"  {CCDTXT2}")
print(f"  {CCDTXT7A}")
