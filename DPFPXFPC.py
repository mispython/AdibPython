def resolve_case_insensitive(directory: Path, target_name: str) -> Path | None:
    """
    Return the actual file path in `directory` whose name matches
    `target_name` case-insensitively, or None if not found.
    """
    target_lower = target_name.lower()
    try:
        for name in os.listdir(directory):
            if name.lower() == target_lower:
                return directory / name
    except OSError:
        pass
    return None
