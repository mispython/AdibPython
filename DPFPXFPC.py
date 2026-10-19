      print(f"  {code} : {cnt}")


# ============================================================================
# CLI
# ============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='BNM Liquidity Report for Trade Finance')
    parser.add_argument('date', nargs='?', help='Report date in YYYY-MM-DD format (default: yesterday)')
    parser.add_argument('--exact', action='store_true')
    parser.add_argument('--latest', action='store_true')
    parser.add_argument('--no-sas7bdat', action='store_true')
    parser.add_argument('--sas-cfg', default=SAS_CONFIG_NAME)

    args = parser.parse_args()

    if args.exact:
        USE_LATEST_FALLBACK = False
    else:
        USE_LATEST_FALLBACK = True

    reptdate = None
    if args.date:
        try:
            reptdate = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD")
            sys.exit(1)

    main(reptdate, write_sas7bdat_flag=not args.no_sas7bdat, sas_cfgname=args.sas_cfg)
