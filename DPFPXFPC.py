def read_sas_dataset(dataset_path, chunksize=None):
    """
    Reads a .sas7bdat file using pyreadstat.
    If chunksize is provided, reads in chunks.
    Returns (pandas.DataFrame, metadata)
    """
    try:
        import pyreadstat
    except ImportError:
        raise ImportError(
            "pyreadstat is required to read .sas7bdat files. "
            "Install it via: pip install pyreadstat"
        )
    
    print(f"[READ] Reading {dataset_path.name}...")
    start_time = datetime.now()
    
    # For large files, read in chunks
    if chunksize and "lnnote" in str(dataset_path).lower():
        print(f"[READ] Reading in chunks of {chunksize} rows...")
        reader = pyreadstat.read_sas7bdat(str(dataset_path), chunksize=chunksize)
        df_list = []
        for df_chunk, meta in reader:
            df_list.append(df_chunk)
            print(f"[READ] Read chunk with {len(df_chunk)} rows")
        df = pd.concat(df_list, ignore_index=True)
        meta = None  # Metadata not available from chunks
    else:
        # Direct read
        df, meta = pyreadstat.read_sas7bdat(str(dataset_path))
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"[READ] Loaded {len(df)} records from {dataset_path.name} in {elapsed:.2f} seconds")
    
    return df, meta
