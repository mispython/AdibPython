#!/usr/bin/env python3
"""
Verify MISOUT.AWSA11 content by reading it back through SAS.
Returns plain-text tables (no HTML markup).
"""

import saspy
import re
from pathlib import Path

OUTPUT_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDAWSA'
LIBREF     = 'MISOUT'
TABLE      = 'AWSA11'


def strip_html(text: str) -> str:
    """Remove HTML tags/entities if the listing came back as HTML."""
    if not text:
        return ''
    # collapse <br>, </tr>, </td> to newlines/whitespace so the table stays readable
    text = re.sub(r'</t[dh]>', ' | ', text, flags=re.IGNORECASE)
    text = re.sub(r'</tr>|<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = (text.replace('&nbsp;', ' ')
                .replace('&amp;', '&')
                .replace('&lt;', '<')
                .replace('&gt;', '>')
                .replace('&quot;', '"'))
    # drop blank lines
    return '\n'.join(line.rstrip() for line in text.splitlines() if line.strip())


def show(title: str, raw: str) -> None:
    print('=' * 78)
    print(title)
    print('=' * 78)
    cleaned = strip_html(raw)
    if cleaned.strip():
        print(cleaned)
    else:
        print('(empty listing — output routed to ODS destination)')
    print()


def main() -> None:
    print("Connecting to SAS ...")
    sas = saspy.SASsession(cfgname='default')

    # 1. Assign the libref
    sas.saslib(LIBREF, path=OUTPUT_DIR)

    # 2. Force classic LISTING so 'LST' contains plain text (not HTML)
    sas.submit("""
        ods _all_ close;
        ods listing;
        options nocenter nodate nonumber linesize=132;
    """)

    # 3. PROC CONTENTS
    r1 = sas.submit(f"""
        proc contents data={LIBREF}.{TABLE} short varnum;
        run;
    """)
    show(f'PROC CONTENTS — {LIBREF}.{TABLE}', r1.get('LST', ''))

    # 4. PROC SQL row/column sanity check
    r2 = sas.submit(f"""
        proc sql;
            select count(*)              as nrows,
                   sum(NOACCT)           as total_accounts,
                   sum(CURBAL)           as total_balance,
                   sum(AVGAMT)           as total_avgamt,
                   sum(ACCYTD)           as total_accytd
            from {LIBREF}.{TABLE};
        quit;
    """)
    show(f'PROC SQL SUMMARY — {LIBREF}.{TABLE}', r2.get('LST', ''))

    # 5. Sample rows
    r3 = sas.submit(f"""
        proc print data={LIBREF}.{TABLE}(obs=5);
            var PURPOSE RACE CUSTCD AVGRNGE DEPRANGE PRODUCT
                NOACCT CURBAL AVGACCT AVGAMT ACCYTD REPTDATE DENOM PRODCD;
        run;
    """)
    show(f'FIRST 5 ROWS — {LIBREF}.{TABLE}', r3.get('LST', ''))

    # 6. Optional: also dump HTML to a file if you want the rendered version
    html_path = Path(OUTPUT_DIR) / f'{TABLE}_contents.html'
    sas.submit(f"""
        ods html file="{html_path}";
        proc contents data={LIBREF}.{TABLE} varnum;
        run;
        ods html close;
        ods listing;
    """)
    if html_path.exists():
        print(f"HTML version also written: {html_path}  ({html_path.stat().st_size:,} bytes)")

    sas.endsas()
    print("Done.")


if __name__ == '__main__':
    main()
