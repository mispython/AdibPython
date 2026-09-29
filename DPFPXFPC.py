    pbif_pl = build_pbif(rv['reptdate'])

    # Convert Polars -> Pandas (and normalise column case + a few dtypes)
    if pbif_pl is None or pbif_pl.is_empty():
        pbif_df = pd.DataFrame()
    else:
        pbif_df = pbif_pl.to_pandas()
        pbif_df.columns = [c.lower() for c in pbif_df.columns]
