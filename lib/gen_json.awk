# =============================================================================
#  gen_json.awk — hierarchical ADABAS-style CDC records as JSON Lines
# =============================================================================
#  Portable POSIX awk (tested with mawk and gawk). One process writes one file
#  slice; lib/generate.sh runs many of them in parallel.
#
#  Variables (-v):
#    defs        path to conf/tables.def
#    tbl         table name
#    mode        base | dose
#    base_rows   rows of the initial (base) load per table
#    k0, k1      slice of the stream to produce: record numbers [k0, k1)
#    dose        dose number (1..n)                          mode=dose only
#    n_upd, n_del, n_ins   changes per dose for this table   mode=dose only
#    hot_pct     updates/deletes hit the newest hot_pct % of base ISNs
#    start_date  first created date (YYYY-MM-DD); span_days  days covered
#    seed        random seed
#
#  Record layout (one line per record):
#    {"hdr":{"isn":..,"op":"I|U|D","ts":"..","batch":..},
#     "rec":{"created":"YYYY-MM-DD", <fields from tables.def> }}
# =============================================================================

BEGIN {
    srand(seed + 0)
    load_word_lists()
    load_defs()
    build_skeleton()

    base_rows += 0; k0 += 0; k1 += 0
    start_day = days_from_civil(start_date)

    if (mode == "dose") {
        # Updates and deletes walk a permutation of the "hot" ISN window, so
        # every ISN changes at most once per dose (a CDC dose is already
        # compacted to the last image per key).
        hot_size = int(base_rows * hot_pct / 100); if (hot_size < 1) hot_size = 1
        hot_lo   = base_rows - hot_size + 1
        stride   = coprime_stride(hot_size)
        offset   = (dose * 7919) % hot_size
        ins_base = base_rows + (dose - 1) * n_ins
        # Dose changes are committed on consecutive days after the base period.
        dose_day = start_day + span_days + dose - 1
    }

    for (k = k0; k < k1; k++) {
        if (mode == "base") {
            isn = k + 1; op = "I"; batch = 0
            cday = created_day(isn)
            ts = fmt_ts(cday, int(rand() * 86400), int(rand() * 1000000))
        } else {
            batch = dose
            if (k < n_upd)              { op = "U"; isn = hot_isn(k) }
            else if (k < n_upd + n_del) { op = "D"; isn = hot_isn(k) }
            else                        { op = "I"; isn = ins_base + (k - n_upd - n_del) + 1 }
            cday = created_day(isn)
            # time-of-day grows with k, so every change in a dose is unique
            ts = fmt_ts(dose_day, int(k / 1000000) % 86400, k % 1000000)
        }
        v[1] = isn; v[2] = "\"" op "\""; v[3] = "\"" ts "\""; v[4] = batch
        v[5] = "\"" fmt_date(cday) "\""
        rec_no++
        for (i = 1; i <= nf; i++) v[HDR + i] = gen_value(i)

        line = piece[0]
        for (i = 1; i <= nv; i++) line = line v[i] piece[i]
        print line
    }
}

# ---------------------------------------------------------------- definitions
function load_defs(    line, a, n) {
    nf = 0
    while ((getline line < defs) > 0) {
        if (line ~ /^F\|/) {
            n = split(line, a, "|")
            if (a[2] != tbl) continue
            nf++
            f_path[nf] = a[3]
            parse_gen(nf, a[6])
        }
    }
    close(defs)
    if (nf == 0) { print "gen_json.awk: no fields for table '" tbl "'" > "/dev/stderr"; exit 2 }
}

function parse_gen(i, spec,    q, a, n, j) {
    f_null[i] = 0
    q = index(spec, "?")
    if (q > 0) { f_null[i] = substr(spec, q + 1) + 0; spec = substr(spec, 1, q - 1) }
    n = split(spec, a, ":")
    f_kind[i] = a[1]; f_a1[i] = a[2]; f_a2[i] = a[3]
    if (f_kind[i] == "pick") { f_npick[i] = split(a[2], a, ";"); for (j = 1; j <= f_npick[i]; j++) PICK[i, j] = a[j] }
    if (f_kind[i] == "date") f_a1[i] = days_from_civil(a[2])
    # container = path without the leaf; NULL decisions are shared by all
    # optional fields of the same group / periodic-group occurrence
    f_cont[i] = f_path[i]; sub(/\.[^.]*$/, "", f_cont[i])
    if (f_cont[i] == f_path[i]) f_cont[i] = ""
}

# Build the JSON text around the values once:  piece[0] v1 piece[1] v2 ...
# Paths are walked in order; containers are opened/closed as the path prefix
# changes. A numeric component means "array element".
function build_skeleton(    i, j, n, m, c, p, common, s, depth, np) {
    HDR = 5
    np = 0
    split("hdr.isn hdr.op hdr.ts hdr.batch rec.created", p, " ")
    for (i = 1; i <= HDR; i++) path[++np] = p[i]
    for (i = 1; i <= nf; i++) path[++np] = "rec." f_path[i]
    nv = np

    s = "{"; depth = 0; cnt[0] = 0; prev_n = 0
    for (i = 1; i <= np; i++) {
        n = split(path[i], c, ".")
        # length of the common container prefix with the previous path
        common = 0
        while (common < n - 1 && common < prev_n - 1 && c[common + 1] == pc[common + 1]) common++
        # close containers deeper than the common prefix
        while (depth > common) { s = s (ctype[depth] == "a" ? "]" : "}"); depth-- }
        # open new containers
        for (j = common + 1; j <= n - 1; j++) {
            s = s (cnt[depth]++ ? "," : "")
            if (ctype[depth] != "a") s = s "\"" c[j] "\":"
            depth++
            ctype[depth] = (c[j + 1] ~ /^[0-9]+$/) ? "a" : "o"
            s = s (ctype[depth] == "a" ? "[" : "{")
            cnt[depth] = 0
        }
        s = s (cnt[depth]++ ? "," : "")
        if (ctype[depth] != "a") s = s "\"" c[n] "\":"
        piece[i - 1] = s; s = ""
        for (j = 1; j <= n; j++) pc[j] = c[j]
        prev_n = n
    }
    while (depth > 0) { s = s (ctype[depth] == "a" ? "]" : "}"); depth-- }
    piece[np] = s "}"
}

# ---------------------------------------------------------------- values
function gen_value(i,    k, key, x) {
    if (f_null[i]) {
        if (f_cont[i] != "") {
            key = f_cont[i]
            if (nd_rec[key] != rec_no) { nd_rec[key] = rec_no; nd_val[key] = (rand() * 100 < f_null[i]) }
            if (nd_val[key]) return "null"
        } else if (rand() * 100 < f_null[i]) return "null"
    }
    k = f_kind[i]
    if (k == "int")     return f_a1[i] + int(rand() * (f_a2[i] - f_a1[i] + 1))
    if (k == "num")     return sprintf("%.2f", f_a1[i] + rand() * (f_a2[i] - f_a1[i]))
    if (k == "ref")     return 1 + int(rand() * base_rows)
    if (k == "bool")    return (rand() < 0.03) ? "true" : "false"
    if (k == "pick")    return "\"" PICK[i, 1 + int(rand() * f_npick[i])] "\""
    if (k == "date")    return "\"" fmt_date(f_a1[i] + int(rand() * f_a2[i])) "\""
    if (k == "id")      return "\"" f_a1[i] sprintf("%0" f_a2[i] "d", isn) "\""
    if (k == "first")   return "\"" FIRST[1 + int(rand() * NFIRST)] "\""
    if (k == "last")    return "\"" LAST[1 + int(rand() * NLAST)] "\""
    if (k == "city")    return "\"" CITY[1 + int(rand() * NCITY)] "\""
    if (k == "country") return "\"" CTRY[1 + int(rand() * NCTRY)] "\""
    if (k == "street")  return "\"" (1 + int(rand() * 250)) " " LAST[1 + int(rand() * NLAST)] " " SFX[1 + int(rand() * 4)] "\""
    if (k == "phone")   return sprintf("\"+%d-%03d-%07d\"", 1 + int(rand() * 98), int(rand() * 1000), int(rand() * 10000000))
    if (k == "email")   return "\"" tolower(FIRST[1 + int(rand() * NFIRST)]) "." isn "@" DOM[1 + int(rand() * 4)] "\""
    if (k == "code")    return "\"" rand_code(f_a1[i] + 0) "\""
    print "gen_json.awk: unknown generator '" k "'" > "/dev/stderr"; exit 2
}

function rand_code(n,    s, j) {
    s = ""
    for (j = 0; j < n; j++) s = s substr(ALNUM, 1 + int(rand() * 36), 1)
    return s
}

# ---------------------------------------------------------------- ISN helpers
# ISNs grow with time, so the created date is a function of the ISN. Inserts
# of later doses get ISNs above base_rows and land in the newest partitions.
function created_day(isn) {
    if (isn <= base_rows) return start_day + int((isn - 1) * span_days / base_rows)
    return start_day + span_days + dose - 1
}
function hot_isn(k) { return hot_lo + (offset + k * stride) % hot_size }
function coprime_stride(n,    s) {
    if (n <= 2) return 1
    for (s = int(n * 0.618) + 1; s > 1; s--) if (gcd(s, n) == 1) return s
    return 1
}
function gcd(a, b,    t) { while (b) { t = a % b; a = b; b = t } return a }

# ---------------------------------------------------------------- dates
# days since 1970-01-01 <-> civil date (proleptic Gregorian, H. Hinnant)
function days_from_civil(s,    y, m, d, era, yoe, doy, doe) {
    y = substr(s, 1, 4) + 0; m = substr(s, 6, 2) + 0; d = substr(s, 9, 2) + 0
    if (m <= 2) y--
    era = int((y >= 0 ? y : y - 399) / 400)
    yoe = y - era * 400
    doy = int((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5) + d - 1
    doe = yoe * 365 + int(yoe / 4) - int(yoe / 100) + doy
    return era * 146097 + doe - 719468
}
function fmt_date(z,    era, doe, yoe, y, doy, mp, d, m) {
    z += 719468
    era = int((z >= 0 ? z : z - 146096) / 146097)
    doe = z - era * 146097
    yoe = int((doe - int(doe / 1460) + int(doe / 36524) - int(doe / 146096)) / 365)
    y = yoe + era * 400
    doy = doe - (365 * yoe + int(yoe / 4) - int(yoe / 100))
    mp = int((5 * doy + 2) / 153)
    d = doy - int((153 * mp + 2) / 5) + 1
    m = mp + (mp < 10 ? 3 : -9)
    if (m <= 2) y++
    return sprintf("%04d-%02d-%02d", y, m, d)
}
function fmt_ts(day, sec, usec) {
    return sprintf("%s %02d:%02d:%02d.%06d", fmt_date(day), int(sec / 3600), int(sec / 60) % 60, sec % 60, usec)
}

# ---------------------------------------------------------------- word lists
function load_word_lists() {
    NFIRST = split("James Mary Robert Patricia John Jennifer Michael Linda David Elizabeth William Barbara Richard Susan Joseph Jessica Thomas Sarah Charles Karen Daniel Lisa Matthew Nancy Noam Maya Ariel Tamar Yosef Shira Hans Greta Pierre Claire Marco Giulia Carlos Lucia Ivan Olga", FIRST, " ")
    NLAST  = split("Smith Johnson Williams Brown Jones Garcia Miller Davis Rodriguez Martinez Hernandez Lopez Wilson Anderson Thomas Taylor Moore Jackson Martin Lee Cohen Levi Mizrahi Peretz Biton Friedman Mueller Schmidt Schneider Fischer Weber Dubois Laurent Rossi Russo Ferrari Silva Santos Ivanov Petrov Novak", LAST, " ")
    NCITY  = split("NEW_YORK LONDON PARIS BERLIN MADRID ROME TEL_AVIV HAIFA ZURICH VIENNA AMSTERDAM BRUSSELS DUBLIN LISBON PRAGUE WARSAW CHICAGO BOSTON TORONTO SYDNEY", CITY, " ")
    NCTRY  = split("US GB FR DE ES IT IL CH AT NL BE IE PT CZ PL CA AU", CTRY, " ")
    split("ST AVE RD BLVD", SFX, " ")
    split("mail.example example.org corp.example bank.example", DOM, " ")
    ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
}
