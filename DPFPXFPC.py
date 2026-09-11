# ============================================================================
# STEP 3: STREAM COMMISSION DATA
# ============================================================================
print("Step 3: Processing commission data (streaming)...")

# Commission file has no ENTITY_CD. The conventional/islamic split is enforced
# upstream on the loan side, so we simply build a single commission lookup
# and let the inner join with loan1 drop any orphan rows.

comm_chunks = []

for chunk in iter_sas_chunks(LOAN_COMM_FILE, COMM_COLS):
    chunk = chunk.with_columns([
        (pl.col('CORGAMT').fill_null(0.0)
         - (pl.col('INTAMT').fill_null(0.0) if 'INTAMT' in chunk.columns else 0.0))
        .alias('NETPROC')
    ]).select(['ACCTNO', 'COMMNO', 'NETPROC'])
    if chunk.height:
        comm_chunks.append(chunk)

comm_data = pl.concat(comm_chunks, how='vertical').unique()
del comm_chunks
gc.collect()

print(f"  Commission rows: {comm_data.height}")

# Join LOAN1 with COMM (only positives)
loan1 = loan1.join(comm_data, on=['ACCTNO', 'COMMNO'], how='inner')
del comm_data
gc.collect()
