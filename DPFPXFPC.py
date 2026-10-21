def run_job():
    from datetime import datetime as dt

    print(f"========== START JOB {JOB_NAME} ==========")

    reptdate = get_reptdate()
    period = compute_period_vars(reptdate)
    print(f"[INFO] REPTDATE={reptdate} -> {period}")

    # SAS naming:
    #   BNM.LOAN&REPTMON1&NOWK1  (prior period)
    #   BNM.LOAN&REPTMON&NOWK    (current period)
    #   BNM.ULOAN&REPTMON&NOWK   (current period)
    # On disk:
    #   loan{REPTMON}{NOWK}.sas7bdat
    #   uloan{REPTMON}{NOWK}{REPTYEAR}.sas7bdat
    loan_prev_path = LOAN_DIR / f"loan{period['REPTMON1']}{period['NOWK1']}.sas7bdat"
    loan_curr_path = LOAN_DIR / f"loan{period['REPTMON']}{period['NOWK']}.sas7bdat"
    uloan_curr_path = ULOAN_DIR / f"uloan{period['REPTMON']}{period['NOWK']}{period['REPTYEAR']}.sas7bdat"

    for path in (loan_prev_path, loan_curr_path, uloan_curr_path):
        disp_shr(path)
        print(f"[SHR] Validated input dataset: {path.name}")

    alw = build_alw(loan_prev_path, loan_curr_path)
    ualw = build_ualw(uloan_curr_path)
    combined = pd.concat([alw, ualw], ignore_index=True)

    alwloan = summarize(combined)

    spool_file = SPOOL_DIR / f"{JOB_NAME}_{dt.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report = build_report(alwloan, period["RDATE"])
    write_sysout(report, spool_file)

    print(f"========== END JOB {JOB_NAME} ==========")
