def coalesce_i(val, default: int = 0) -> int:
    """NaN/None-safe integer conversion."""
    if val is None:
        return default
    if isinstance(val, float) and val != val:   # NaN
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default
