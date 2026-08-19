def list_input_files():
    """List all files in input directories to help with debugging"""
    print("\n" + "=" * 60)
    print("INPUT FILES")
    print("=" * 60)
    
    for key, path in PATHS.items():
        if key in ['OUTPUT', 'DEPOSIX']:
            continue
        
        p = Path(path)
        if p.exists():
            print(f"\n{key} directory ({path}):")
            try:
                files = sorted([f.name for f in p.iterdir() if f.is_file()])
                for f in files:
                    print(f"  - {f}")
            except Exception as e:
                print(f"  Error listing files: {e}")
        else:
            print(f"\n{key} directory does not exist ({path})")
