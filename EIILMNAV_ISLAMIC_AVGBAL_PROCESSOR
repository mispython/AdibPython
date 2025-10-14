# EIILMNAV_AVGBAL_PROCESSOR.py

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
from pathlib import Path
import os

def main():
    # Configuration using pathlib
    base_path = Path(".")
    ln_path = base_path / "LN"
    mis_path = base_path / "MIS"
    
    # Create output directory if it doesn't exist
    mis_path.mkdir(exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect()
    
    # Step 1: Process REPTDATE and extract parameters
    reptdate_query = f"""
    SELECT * FROM read_parquet('{ln_path / "REPTDATE.parquet"}')
    """
    reptdate_df = conn.execute(reptdate_query).arrow()
    
    if len(reptdate_df) > 0:
        reptdate = reptdate_df['REPTDATE'][0].as_py()
        prev_day = (reptdate - pa.scalar(1, pa.duration('day')).as_py()).strftime('%d')
        rept_day = reptdate.strftime('%d')
        rept_month = reptdate.strftime('%m')
        rdate = reptdate.strftime('%Y-%m-%d')
        
        prev_day_int = int(prev_day)
        rept_day_int = int(rept_day)
        rept_month_int = int(rept_month)
        
        print(f"Processing date: {reptdate}")
        print(f"Previous Day: {prev_day}, Current Day: {rept_day}, Month: {rept_month}")
    else:
        raise ValueError("No data found in REPTDATE table")
    
    # Step 2: Get today's balances
    today_bal_query = f"""
    SELECT 
        ACCTNO,
        NOTENO,
        CAST(BALANCE as DOUBLE) as BALANCE
    FROM read_parquet('{ln_path / "LNNOTE.parquet"}')
    """
    
    today_bal_df = conn.execute(today_bal_query).arrow()
    
    # Step 3: Implement the GET_AVGBAL macro logic
    if rept_day_int == 1:
        # First day of month - initialize averages
        main_avgbal_query = f"""
        SELECT 
            ACCTNO,
            NOTENO,
            BALANCE as BAL{rept_day},
            BALANCE as MTDAVBAL_MIS,
            BALANCE as LAST_AVGBAL,
            1 as LAST_DAY
        FROM today_bal_df
        """
        
        main_avgbal_df = conn.execute(main_avgbal_query).arrow()
        
    else:
        # Days 2+ - process with previous data
        prev_month_file = mis_path / f"LNVG_{rept_month}.parquet"
        
        if prev_month_file.exists():
            # Read previous month's data
            main_avgbal_query = f"""
            SELECT * FROM read_parquet('{prev_month_file}')
            """
            main_avgbal_df = conn.execute(main_avgbal_query).arrow()
        else:
            # Create empty structure if no previous data
            main_avgbal_df = pa.table({
                'ACCTNO': pa.array([], pa.int64()),
                'NOTENO': pa.array([], pa.int64()),
                'LAST_AVGBAL': pa.array([], pa.float64()),
                'LAST_DAY': pa.array([], pa.int32())
            })
        
        # Prepare today's balances
        temp_bal_query = f"""
        SELECT 
            ACCTNO,
            NOTENO,
            BALANCE as BAL{rept_day}
        FROM today_bal_df
        """
        
        temp_bal_df = conn.execute(temp_bal_query).arrow()
        
        # Merge and process data
        if len(main_avgbal_df) > 0 and len(temp_bal_df) > 0:
            # Merge existing data with today's balances
            merge_query = f"""
            WITH merged_data AS (
                SELECT 
                    COALESCE(m.ACCTNO, t.ACCTNO) as ACCTNO,
                    COALESCE(m.NOTENO, t.NOTENO) as NOTENO,
                    m.LAST_AVGBAL,
                    m.LAST_DAY,
                    t.BAL{rept_day}
                FROM main_avgbal_df m
                FULL OUTER JOIN temp_bal_df t 
                ON m.ACCTNO = t.ACCTNO AND m.NOTENO = t.NOTENO
            )
            SELECT * FROM merged_data
            """
            
            merged_df = conn.execute(merge_query).arrow()
            
            # Process the complex logic for rerun and average calculation
            # This requires Python processing due to dynamic column references
            processed_records = []
            
            for record in merged_df.to_pylist():
                acctno = record['ACCTNO']
                noteno = record['NOTENO']
                last_avgbal = record.get('LAST_AVGBAL')
                last_day = record.get('LAST_DAY')
                curr_bal = record.get(f'BAL{rept_day}')
                
                # Handle rerun logic
                if last_day and last_day > prev_day_int:
                    # Rerun scenario - need to recalculate from the gap
                    temp_last_avgbal = last_avgbal
                    temp_last_day = last_day
                    
                    while temp_last_day > prev_day_int:
                        # Find the balance for that day (this would need the actual historical data)
                        # For now, we'll use a simplified approach
                        day_col = f"BAL{temp_last_day:02d}"
                        curr_bal_day = record.get(day_col)
                        
                        if curr_bal_day is not None:
                            temp_last_avgbal = ((temp_last_avgbal * temp_last_day) - curr_bal_day) / (temp_last_day - 1)
                        
                        temp_last_day -= 1
                    
                    last_avgbal = temp_last_avgbal
                    last_day = temp_last_day
                
                # Calculate new averages
                if last_avgbal is not None and curr_bal is not None:
                    mtdavgbal_mis = (last_avgbal * (rept_day_int - 1) + curr_bal) / rept_day_int
                    new_last_avgbal = mtdavgbal_mis
                    new_last_day = rept_day_int
                elif curr_bal is not None:
                    # New record
                    mtdavgbal_mis = curr_bal
                    new_last_avgbal = curr_bal
                    new_last_day = 1
                else:
                    # Existing record without today's data
                    mtdavgbal_mis = last_avgbal
                    new_last_avgbal = last_avgbal
                    new_last_day = last_day if last_day else prev_day_int
                
                # Ensure LAST_DAY is properly set
                if new_last_day is None:
                    new_last_day = prev_day_int
                
                output_record = {
                    'ACCTNO': acctno,
                    'NOTENO': noteno,
                    f'BAL{rept_day}': curr_bal,
                    'MTDAVBAL_MIS': mtdavgbal_mis,
                    'LAST_AVGBAL': new_last_avgbal,
                    'LAST_DAY': new_last_day
                }
                
                # Add any existing balance columns
                for key, value in record.items():
                    if key.startswith('BAL') and key not in output_record:
                        output_record[key] = value
                
                processed_records.append(output_record)
            
            main_avgbal_df = pa.Table.from_pylist(processed_records)
        else:
            # Handle case where one of the datasets is empty
            if len(temp_bal_df) > 0:
                main_avgbal_df = temp_bal_df
                # Initialize new records
                records = main_avgbal_df.to_pylist()
                for record in records:
                    record['MTDAVBAL_MIS'] = record[f'BAL{rept_day}']
                    record['LAST_AVGBAL'] = record[f'BAL{rept_day}']
                    record['LAST_DAY'] = rept_day_int
                main_avgbal_df = pa.Table.from_pylist(records)
    
    # Step 4: Save the main average balance data
    output_file_main = mis_path / f"LNVG_{rept_month}.parquet"
    pq.write_table(main_avgbal_df, output_file_main)
    
    # Step 5: Create the final output with only required columns
    final_output_query = """
    SELECT 
        ACCTNO,
        NOTENO,
        MTDAVBAL_MIS
    FROM main_avgbal_df
    ORDER BY ACCTNO, NOTENO
    """
    
    final_output_df = conn.execute(final_output_query).arrow()
    
    output_file_final = mis_path / f"LNVG{rept_month}.parquet"
    pq.write_table(final_output_df, output_file_final)
    
    # Also create CSV output
    csv.write_csv(final_output_df, mis_path / f"LNVG{rept_month}.csv")
    
    # Print summary statistics
    print(f"\nProcessing completed!")
    print(f"Total records processed: {len(main_avgbal_df)}")
    print(f"Final output records: {len(final_output_df)}")
    print(f"Output files created:")
    print(f"  - {output_file_main}")
    print(f"  - {output_file_final}")
    print(f"  - {mis_path / f'LNVG{rept_month}.csv'}")
    
    # Summary statistics
    if len(final_output_df) > 0:
        mtdavg_sum = sum(final_output_df.column('MTDAVBAL_MIS').to_pylist())
        mtdavg_avg = mtdavg_sum / len(final_output_df)
        print(f"\nSummary Statistics:")
        print(f"  Total MTDAVBAL_MIS: {mtdavg_sum:,.2f}")
        print(f"  Average MTDAVBAL_MIS: {mtdavg_avg:,.2f}")
        print(f"  Min MTDAVBAL_MIS: {min(final_output_df.column('MTDAVBAL_MIS').to_pylist()):,.2f}")
        print(f"  Max MTDAVBAL_MIS: {max(final_output_df.column('MTDAVBAL_MIS').to_pylist()):,.2f}")
    
    conn.close()

if __name__ == "__main__":
    main()
