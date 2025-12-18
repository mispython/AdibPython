  Below line like 90% similar, when submit sonaqube, it will flag error  please use another method to handle it. 

def convert_yyyymmdd_to_sas_date(date_series):
    """Convert YYYYMMDD format to SAS date"""
    result = pd.Series([np.nan] * len(date_series), dtype='float64')
    
    for idx, val in date_series.items():
        if pd.isna(val) or val == 0:
            result[idx] = np.nan
        else:
            try:
                date_str = str(int(val))
                if len(date_str) == 8:
                    date_obj = pd.to_datetime(date_str, format='%Y%m%d')
                    sas_epoch = pd.Timestamp('1960-01-01')
                    result[idx] = (date_obj - sas_epoch).days
                else:
                    result[idx] = np.nan
            except:
                result[idx] = np.nan
    
    return result

def convert_yymmdd_to_sas_date(date_series):
    """Convert YYMMDD format to SAS date (assumes 20xx for YY)"""
    result = pd.Series([np.nan] * len(date_series), dtype='float64')
    
    for idx, val in date_series.items():
        if pd.isna(val) or val == 0:
            result[idx] = np.nan
        else:
            try:
                date_str = str(int(val)).zfill(6)
                if len(date_str) == 6:
                    # Add century: assume 20xx
                    full_date_str = '20' + date_str
                    date_obj = pd.to_datetime(full_date_str, format='%Y%m%d')
                    sas_epoch = pd.Timestamp('1960-01-01')
                    result[idx] = (date_obj - sas_epoch).days
                else:
                    result[idx] = np.nan
            except:
                result[idx] = np.nan
    
    return result
