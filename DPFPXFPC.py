original sas code:

*;
DATA REPTDATE;
   SET MNITB.REPTDATE;
   CALL SYMPUT('REPTYEAR',PUT(REPTDATE,YEAR4.));
   CALL SYMPUT('REPTMON',PUT(MONTH(REPTDATE),Z2.));
   CALL SYMPUT('REPTDAY',PUT(DAY(REPTDATE),Z2.));
   CALL SYMPUT('RDATE',PUT(REPTDATE,DDMMYY8.));
RUN;
PROC PRINT; FORMAT REPTDATE DDMMYY10.;
%INC PGM(PBBDPFMT,PBBELF);
*;
DATA CAMII CAMIC
     CAMCI CAMCC
     CAMFYI CAMFYC;
  SET OMY.CAMV&REPTDAY&REPTMON;
  BRABV=PUT(BRANCH,BRCHCD.);
  CUSTCD = PUT(CUSTCODE,DDCUSTCD.);
  IF PRODUCT NOT IN (79,80,413) AND
     CUSTCD NOT IN (02,03,07,10,12,81,82,83,84);
  IF PRODUCT IN (400:411,420:431,432:434) AND CURCODE NE 'MYR' THEN DO;
     IF CUSTCODE IN (77,78,95,96) THEN OUTPUT CAMFYI;
                                  ELSE OUTPUT CAMFYC;
  END;
  ELSE DO;
     IF CUSTCODE IN (77,78,95,96) THEN DO;
        IF (3000<=COSTCTR<=3999)  THEN OUTPUT CAMII;
                                  ELSE OUTPUT CAMIC;
     END;
     ELSE DO;
        IF (3000<=COSTCTR<=3999) AND
                NOT (3790000000<=ACCTNO<=3799999999) THEN OUTPUT CAMCI;
        ELSE IF NOT (3000<=COSTCTR<=3999) AND
                NOT (3590000000<=ACCTNO<=3599999999)
                                                     THEN OUTPUT CAMCC;
     END;
  END;
*;
DATA CAMFYI;
   SET CAMFYI END=LAST;
   FILE CAMFYI;
   IF _N_=1 THEN DO;
   PUT @001 'CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (FOREIGN CURRENCY) INDIVIDUAL';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   TNETBALC+NETBALC;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALC ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP ';' TNETBALC;
   END;
*;
DATA CAMFYC;
   SET CAMFYC END=LAST;
   FILE CAMFYC;
   IF _N_=1 THEN DO;
   PUT @001 'CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (FOREIGN CURRENCY) CORPORATE';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   TNETBALC+NETBALC;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALC ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP ';' TNETBALC;
   END;
*;
DATA CAMII;
   SET CAMII END=LAST;
   FILE CAMII;
   IF _N_=1 THEN DO;
   PUT @001 'CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (INDIVIDUAL CUSTOMERS - ISLAMIC)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALC ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA CAMIC;
   SET CAMIC END=LAST;
   FILE CAMIC;
   IF _N_=1 THEN DO;
   PUT @001 'CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (INDIVIDUAL CUSTOMERS-CONVENTIONAL)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALC ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA CAMCI;
   SET CAMCI END=LAST;
   FILE CAMCI;
   IF _N_=1 THEN DO;
   PUT @001 'CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (CORPORATE CUSTOMERS - ISLAMIC)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALC ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA CAMCC;
   SET CAMCC END=LAST;
   FILE CAMCC;
   IF _N_=1 THEN DO;
   PUT @001 'CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (CORPORATE CUSTOMERS - CONVENTIONAL)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALC ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA FDMII FDMIC
     FDMCI FDMCC
     FDMFYI FDMFYC;
  SET OMY.FDMV&REPTDAY&REPTMON;
  BRABV=PUT(BRANCH,BRCHCD.);
  IF (350<=PRODUCT<=362)  THEN DO;
     IF CUSTCODE IN (77,78,95,96) THEN OUTPUT FDMFYI;
                                  ELSE OUTPUT FDMFYC;
  END;
  ELSE DO;
     IF CUSTCODE IN (77,78,95,96) THEN DO;
        IF (3000<=COSTCTR<=3999)  THEN OUTPUT FDMII;
                                  ELSE OUTPUT FDMIC;
     END;
     ELSE DO;
        IF (3000<=COSTCTR<=3999)  THEN OUTPUT FDMCI;
                                  ELSE OUTPUT FDMCC;
     END;
  END;
*;
DATA FDMFYI;
   SET FDMFYI END=LAST;
   FILE FDMFYI;
   IF _N_=1 THEN DO;
   PUT @001 'FIXED DEPOSIT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (FOREIGN CURRENCY) INDIVIDUAL';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALF ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA FDMFYC;
   SET FDMFYC END=LAST;
   FILE FDMFYC;
   IF _N_=1 THEN DO;
   PUT @001 'FIXED DEPOSIT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (FOREIGN CURRENCY) CORPORATE';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALF ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA FDMII;
   SET FDMII END=LAST;
   FILE FDMII;
   IF _N_=1 THEN DO;
   PUT @001 'FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @058 'BY BRANCH (INDIVIDUAL CUSTOMERS - ISLAMIC)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALF;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALF ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA FDMIC;
   SET FDMIC END=LAST;
   FILE FDMIC;
   IF _N_=1 THEN DO;
   PUT @001 'FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @054 'BY BRANCH (INDIVIDUAL CUSTOMERS - CONVENTIONAL)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALF;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALF ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA FDMCI;
   SET FDMCI END=LAST;
   FILE FDMCI;
   IF _N_=1 THEN DO;
   PUT @001 'FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @054 'BY BRANCH (CORPORATE CUSTOMERS - ISLAMIC)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALF;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALF ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;
*;
DATA FDMCC;
   SET FDMCC END=LAST;
   FILE FDMCC;
   IF _N_=1 THEN DO;
   PUT @001 'FD ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT'
       @054 'BY BRANCH (CORPORATE CUSTOMERS - CONVENTIONAL)';
   PUT @001 'AS AT ' "&RDATE";
   PUT @001 'BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALF;CUSTCODE';
   END;
   TCURBAL+CURBAL;
   TCURBALP+PCURBAL;
   PUT @001 BRANCH ';' BRABV ';' NAME ';' ACCTNO ';' CURBAL ';'
            PCURBAL ';' NETBALF ';' CUSTCODE;
   IF LAST THEN DO;
      PUT @001 ';;;;' TCURBAL ';' TCURBALP;
   END;

*;



converted python equivalent:

import duckdb
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Format mappings (PBBDPFMT, PBBELF)
BRCHCD = {}      # Branch code to abbreviation
DDCUSTCD = {}    # Customer code format

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/mnitb/reptdate.parquet')").fetchone()[0]
reptyear, reptmon, reptday = str(reptdate.year), f"{reptdate.month:02d}", f"{reptdate.day:02d}"
rdate = reptdate.strftime('%d/%m/%Y')

print(f"Report Date: {rdate}")

# ============================================================================
# PROCESS CURRENT ACCOUNT MOVEMENTS (CAMV)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE camv AS
    SELECT *, 
           branch brabv,  -- Apply BRCHCD format if available
           CASE WHEN custcode IN (2,3,7,10,12,81,82,83,84) THEN 1 ELSE 0 END exclude,
           CASE WHEN product BETWEEN 400 AND 411 OR product BETWEEN 420 AND 431 OR product BETWEEN 432 AND 434
                THEN 1 ELSE 0 END fy_product,
           CASE WHEN custcode IN (77,78,95,96) THEN 'I' ELSE 'C' END cust_type,
           CASE WHEN costctr BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END bank_type
    FROM read_parquet('{INPUT_DIR}/omy/camv{reptday}{reptmon}.parquet')
    WHERE product NOT IN (79,80,413)
      AND custcode NOT IN (2,3,7,10,12,81,82,83,84)
""")

# Split into 6 categories
categories = {
    'CAMFYI': ('fy_product=1 AND curcode!=\'MYR\' AND cust_type=\'I\'', 
               'FOREIGN CURRENCY) INDIVIDUAL', 'NETBALC'),
    'CAMFYC': ('fy_product=1 AND curcode!=\'MYR\' AND cust_type=\'C\'', 
               'FOREIGN CURRENCY) CORPORATE', 'NETBALC'),
    'CAMII':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'I\'', 
               'INDIVIDUAL CUSTOMERS - ISLAMIC)', 'NETBALC'),
    'CAMIC':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'C\'', 
               'INDIVIDUAL CUSTOMERS-CONVENTIONAL)', 'NETBALC'),
    'CAMCI':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'I\' AND acctno NOT BETWEEN 3790000000 AND 3799999999', 
               'CORPORATE CUSTOMERS - ISLAMIC)', 'NETBALC'),
    'CAMCC':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'C\' AND acctno NOT BETWEEN 3590000000 AND 3599999999', 
               'CORPORATE CUSTOMERS - CONVENTIONAL)', 'NETBALC')
}

for fname, (cond, desc, netcol) in categories.items():
    data = con.execute(f"SELECT * FROM camv WHERE {cond}").fetchall()
    
    with open(OUTPUT_DIR/f'{fname.lower()}.csv', 'w') as f:
        f.write(f"CURRENT ACCOUNT MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT BY BRANCH ({desc}\n")
        f.write(f"AS AT {rdate}\n")
        f.write("BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALC;CUSTCODE\n")
        
        tcurbal = tpcurbal = tnetbal = 0.0
        for r in data:
            f.write(f"{r[1]};{r[-4]};{r[2]};{r[0]};{r[3]};{r[4]};{r[5]};{r[6]}\n")
            tcurbal += r[3]
            tpcurbal += r[4]
            if netcol == 'NETBALC':
                tnetbal += r[5]
        
        if netcol == 'NETBALC':
            f.write(f";;;;{tcurbal};{tpcurbal};{tnetbal}\n")
        else:
            f.write(f";;;;{tcurbal};{tpcurbal}\n")
    
    print(f"{fname}: {len(data)} records")

# ============================================================================
# PROCESS FIXED DEPOSIT MOVEMENTS (FDMV)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE fdmv AS
    SELECT *, 
           branch brabv,
           CASE WHEN product BETWEEN 350 AND 362 THEN 1 ELSE 0 END fy_product,
           CASE WHEN custcode IN (77,78,95,96) THEN 'I' ELSE 'C' END cust_type,
           CASE WHEN costctr BETWEEN 3000 AND 3999 THEN 'I' ELSE 'C' END bank_type
    FROM read_parquet('{INPUT_DIR}/omy/fdmv{reptday}{reptmon}.parquet')
""")

fd_categories = {
    'FDMFYI': ('fy_product=1 AND cust_type=\'I\'', 
               'FOREIGN CURRENCY) INDIVIDUAL'),
    'FDMFYC': ('fy_product=1 AND cust_type=\'C\'', 
               'FOREIGN CURRENCY) CORPORATE'),
    'FDMII':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'I\'', 
               'INDIVIDUAL CUSTOMERS - ISLAMIC)'),
    'FDMIC':  ('fy_product=0 AND cust_type=\'I\' AND bank_type=\'C\'', 
               'INDIVIDUAL CUSTOMERS - CONVENTIONAL)'),
    'FDMCI':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'I\'', 
               'CORPORATE CUSTOMERS - ISLAMIC)'),
    'FDMCC':  ('fy_product=0 AND cust_type=\'C\' AND bank_type=\'C\'', 
               'CORPORATE CUSTOMERS - CONVENTIONAL)')
}

for fname, (cond, desc) in fd_categories.items():
    data = con.execute(f"SELECT * FROM fdmv WHERE {cond}").fetchall()
    
    title = 'FIXED DEPOSIT' if fname.startswith('FDMFY') else 'FD ACCOUNT'
    
    with open(OUTPUT_DIR/f'{fname.lower()}.csv', 'w') as f:
        f.write(f"{title} MOVEMENTS OF RM 1MIL & ABOVE PER ACCOUNT BY BRANCH ({desc}\n")
        f.write(f"AS AT {rdate}\n")
        f.write("BRANCH;BRABV;NAME;ACCTNO;CURBAL;PCURBAL;NETBALF;CUSTCODE\n")
        
        tcurbal = tpcurbal = 0.0
        for r in data:
            f.write(f"{r[1]};{r[-3]};{r[2]};{r[0]};{r[3]};{r[4]};{r[5]};{r[6]}\n")
            tcurbal += r[3]
            tpcurbal += r[4]
        
        f.write(f";;;;{tcurbal};{tpcurbal}\n")
    
    print(f"{fname}: {len(data)} records")

con.close()
print(f"\nCompleted: 12 CSV reports generated in {OUTPUT_DIR}")


  
proceed with the python program but change and modify some. all inputs are in sas7bdat sas dataset. use pyreadstat to read remove reptdate, use datetime timedelta - 1 instead. output in text files
