EXPECTED_LEN = 797

def _line_wofftex1(r):
    buf = [' '] * EXPECTED_LEN

    def put(pos, s, n):
        s = ('' if s is None else str(s)).replace('\n', ' ').replace('\r', ' ')[:n].ljust(n)
        start = pos - 1
        buf[start:start+n] = list(s)

    def put_num(pos, v, n, d=2):
        try: v = 0.0 if v is None else float(v)
        except Exception: v = 0.0
        s = f"{v:.{d}f}".rjust(n)
        if len(s) > n: s = s[:n]
        put(pos, s, n)

    def put_int(pos, v, n):
        try: v = 0 if v is None else int(float(v))
        except Exception: v = 0
        put(pos, str(v).rjust(n), n)

    put(1,   r.get('BRANCH'), 7)
    put(9,   r.get('NAME'), 40)
    put_num(50, r.get('ACCTNO'), 10, 0)
    put_num(61, r.get('NOTENO'), 5, 0)
    put(67,  r.get('BORSTAT'), 1)
    put_num(69,  r.get('IIS'), 16)
    put_num(85,  r.get('OI'), 16)
    put_num(101, r.get('TOTIIS'), 16)
    put_num(117, r.get('SP'), 16)
    put_num(149, r.get('CURBAL'), 16)
    put_num(165, r.get('PREVBAL'), 16)
    put_num(181, r.get('PAYMENT'), 16)
    put_num(197, r.get('ECSRRSRV'), 16)
    put_num(213, r.get('POSTAMT'), 16)
    put_num(229, r.get('OTHERAMT'), 16)
    put(245, r.get('MATDATE'), 10)
    put_int(255, r.get('LOANTYPE'), 3)
    put_num(258, r.get('INTAMT'), 16)
    put(274, r.get('POSTNTRN'), 1)
    put_num(278, r.get('MARKETVL'), 16)
    put_num(294, r.get('INTEARN4'), 16)
    put_int(310, r.get('DAYS'), 6)
    put_int(317, r.get('CUSTCODE'), 3)
    put(321, r.get('RIND'), 1)
    put_num(322, r.get('OIFEEAMT'), 16)
    put(339, r.get('LASTTRA1'), 10)
    put_int(350, r.get('LSTTRNCD'), 3)
    put_int(354, r.get('MTHPDUE'), 3)
    put_num(357, r.get('BALANCE'), 16)
    put(408, r.get('GUAREND'), 20)
    put(429, r.get('GUARNAM1'), 40)
    put(470, r.get('GUARNAM2'), 40)
    iss = r.get('ISSXDTE')
    iss_s = fmt_mmddyy10(iss) if isinstance(iss, date) else (iss or '')
    put(511, iss_s, 10)
    put_num(522, r.get('NETPROC'), 16)
    put(539, r.get('COLLDESC'), 70)
    put_int(610, r.get('COLLYEAR'), 4)
    put_int(615, r.get('BILPAID'), 3)
    put(619, r.get('CRRGRADE'), 5)
    put_num(625, r.get('MARGINFI'), 16)
    put_int(642, r.get('NOTETERM'), 3)
    put_num(646, r.get('PAYAMT'), 16)
    put(663, r.get('DOBMNI'), 10)
    put(674, r.get('ECSRIND'), 1)
    put(677, r.get('DELQCD'), 2)
    put(713, r.get('OCCUPAT'), 3)
    put(743, r.get('BGC'), 2)
    put(767, r.get('PAY75PCT'), 1)
    put(769, r.get('NACODATE'), 10)
    put(780, r.get('CP'), 1)
    put(782, r.get('MODELDES'), 6)
    put(789, r.get('AKPK_STATUS'), 9)

    return ''.join(buf)
