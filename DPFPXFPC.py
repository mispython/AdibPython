import paramiko

DRR_HOST_DESC       = "Data Report Repository (DRR)"
DRR_REMOTE_DIR      = "WMTB-SA&S/DEP - SAS/MOVEMENT"

SFTP_CONTROL_FILE   = Path('/sasdata/dwh/control/ctl_dwh_sftp_info.sas7bdat')

# ============================================================================
# SFTP UPLOAD
# ============================================================================

def get_sftp_info(host_desc: str):
    """Return (SFTP_ID, SFTP_PW, HOST_IP, HOST_KEY) for the row matching HOST_DESC."""
    if not SFTP_CONTROL_FILE.exists():
        raise FileNotFoundError(f"SFTP control file not found: {SFTP_CONTROL_FILE}")

    ctl, _meta = pyreadstat.read_sas7bdat(str(SFTP_CONTROL_FILE))
    ctl.columns = [str(c).upper().strip() for c in ctl.columns]

    required = {"HOST_DESC", "SFTP_ID", "SFTP_PW", "HOST_IP"}
    missing_columns = sorted(required.difference(ctl.columns))
    if missing_columns:
        raise RuntimeError(
            "SFTP control file is missing columns: " + ", ".join(missing_columns)
        )

    descriptions = ctl["HOST_DESC"].fillna("").astype(str).str.strip()
    rows = ctl[descriptions.eq(host_desc)]
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one SFTP row for '{host_desc}'; found {len(rows)}"
        )

    row = rows.iloc[0]

    def clean(value):
        return str(value).strip() if pd.notna(value) else ""

    sftp_id  = clean(row["SFTP_ID"])
    sftp_pw  = clean(row["SFTP_PW"])
    host_ip  = clean(row["HOST_IP"])
    host_key = clean(row["HOST_KEY"]) if "HOST_KEY" in ctl.columns else ""

    empty_values = [
        name for name, value in (
            ("SFTP_ID", sftp_id), ("SFTP_PW", sftp_pw), ("HOST_IP", host_ip)
        ) if not value
    ]
    if empty_values:
        raise RuntimeError(
            f"SFTP row for '{host_desc}' has blank values: {', '.join(empty_values)}"
        )

    return sftp_id, sftp_pw, host_ip, host_key


def sftp_upload(host_desc: str, remote_dir: str, local_file: Path,
                remote_name: str) -> None:
    """Upload one file to the SFTP server identified by HOST_DESC.

    Uses text-mode sftp.open(remote_name, 'w') instead of sftp.put() so the
    server negotiates line endings, mirroring the SAS FTP 'mode=text' step.
    """
    if not local_file.exists():
        raise FileNotFoundError(f"Local report missing: {local_file}")

    sftp_id, sftp_pw, host_ip, _host_key = get_sftp_info(host_desc)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    print(f"  Connecting to {host_desc} ({host_ip}) as {sftp_id} ...")
    try:
        ssh.connect(hostname=host_ip, username=sftp_id, password=sftp_pw)
    except Exception as exc:
        raise ConnectionError(
            f"Could not connect to {host_desc} ({host_ip}): {exc}"
        ) from exc

    try:
        sftp = ssh.open_sftp()
        try:
            try:
                sftp.chdir(remote_dir)
            except IOError as exc:
                raise FileNotFoundError(
                    f"Remote folder '{remote_dir}' not found on {host_desc}"
                ) from exc

            print(f"  SFTP upload (text mode): {local_file.name} -> "
                  f"{posixpath.join(remote_dir, remote_name)}")

            with open(local_file, 'r', newline='') as src:
                content = src.read()
            with sftp.open(remote_name, 'w') as dst:
                dst.write(content)

            remote_size = sftp.stat(remote_name).st_size
            local_size  = local_file.stat().st_size
            if remote_size != local_size:
                print(f"  NOTE: size differs for {remote_name} "
                      f"(local={local_size}, remote={remote_size}) - "
                      f"expected if the server translated newlines.")
        finally:
            sftp.close()
    finally:
        ssh.close()


def sftp_to_drr(file_pairs: list) -> None:
    """JCL step: FTP TO SAS DATAWAREHOUSE (DRR).

    Each element is a (local_path, remote_name) tuple. Since the local file
    and remote name are derived from the same report_filename() call, they
    are identical by construction.
    """
    print("\nUploading to DRR via SFTP...")
    for local_file, remote_name in file_pairs:
        sftp_upload(
            DRR_HOST_DESC,
            DRR_REMOTE_DIR,
            local_file,
            remote_name,
        )
    print("  DRR upload complete.")
