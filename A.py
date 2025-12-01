con.execute(f"""
            CREATE TEMP TABLE hist_with_dates AS
            SELECT 
                ACCTNO, 
                NOTENO,
                -- Convert BILL_DT from SAS numeric to DATE
                CASE 
                    WHEN BILL_DT IS NOT NULL THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(BILL_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS BILL_DT,
                -- Convert BILL_PAID_DT from SAS numeric to DATE - FIXED: Compare numeric values first
                CASE 
                    WHEN BILL_PAID_DT IS NOT NULL AND CAST(BILL_PAID_DT AS INTEGER) > 0 THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(BILL_PAID_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS BILL_PAID_DT,
                BILL_AMT, 
                BILL_AMT_PRIN, 
                BILL_AMT_INT, 
                BILL_AMT_ESCROW, 
                BILL_AMT_FEE,
                BILL_NOT_PAY_AMT, 
                BILL_NOT_PAY_AMT_PRIN, 
                BILL_NOT_PAY_AMT_INT,
                BILL_NOT_PAY_AMT_ESCROW, 
                BILL_NOT_PAY_AMT_FEE,
                COSTCTR, 
                PRODUCT,
                -- Convert VALID_FROM_DT from SAS numeric to DATE
                CASE 
                    WHEN VALID_FROM_DT IS NOT NULL THEN 
                        DATE '{SAS_ORIGIN.date()}' + INTERVAL (CAST(VALID_FROM_DT AS INTEGER)) DAYS
                    ELSE NULL 
                END AS VALID_FROM_DT
            FROM loan_bill_hist_active
        """)
