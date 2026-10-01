# =============================================================================
#  gen_changes.awk — ADABAS-style CDC change records (JSON Lines)
# =============================================================================
#  Portable POSIX awk (mawk, gawk, busybox). One process writes one file;
#  generate.sh runs several in parallel.
#
#  Variables (-v):
#    base_rows        rows of the fact table (ISNs 1 … base_rows)
#    n_upd n_del n_ins  changes of each kind (in total, over all files)
#    k0, k1           slice of the change stream written by this process
#    hot_pct          updates/deletes hit the newest hot_pct % of the ISNs
#    start_date span_days   created_date of ISN 1, days covered by the table
#    seed             random seed
#
#  Change k (0 … n_upd+n_del+n_ins-1):
#    k <  n_upd             update of an existing ISN  (op "U")
#    k <  n_upd + n_del     delete of an existing ISN  (op "D")
#    otherwise              insert of a new ISN above base_rows (op "I")
#  Updates and deletes walk a permutation of the hot window, so every ISN
#  appears at most once (a CDC batch is already compacted per key).
#
#  One record (one line):
#  {"hdr":{"isn":…,"op":"U","ts":"…","batch":1},
#   "rec":{"created":"…","txn_ref":"…","acct_isn":…,"card_isn":…,"type":"…",
#          "amount":…,"currency":"…","booking_date":"…",
#          "merchant":{"mcc":…,"name":"…","city":"…","country":"…"},   group (optional)
#          "tag":["…","…"],                                            MU (0-2 values)
#          "reversal":false}}
# =============================================================================

BEGIN {
    srand(seed + 0)
    base_rows += 0; k0 += 0; k1 += 0; n_upd += 0; n_del += 0; n_ins += 0
    split("POS ATM XFER FEE INT DD", TYPE, " ")
    split("USD EUR GBP ILS CHF", CURR, " ")
    split("Cohen Smith Rossi Dubois Mueller Levi Garcia Novak", MNAME, " ")
    split("TEL_AVIV LONDON ROME PARIS BERLIN HAIFA MADRID PRAGUE", MCITY, " ")
    split("IL GB IT FR DE IL ES CZ", MCTRY, " ")
    split("ONLINE CONTACTLESS RECURRING FOREIGN PROMO", TAG, " ")

    start_day = days_from_civil(start_date)
    change_day = start_day + span_days          # the day after the base period
    hot_size = int(base_rows * hot_pct / 100); if (hot_size < 1) hot_size = 1
    hot_lo   = base_rows - hot_size + 1
    stride   = coprime_stride(hot_size)
    n_acct = int(base_rows / 20) + 1
    n_card = int(base_rows / 10) + 1

    for (k = k0; k < k1; k++) {
        if (k < n_upd)              { op = "U"; isn = hot_lo + (k * stride) % hot_size }
        else if (k < n_upd + n_del) { op = "D"; isn = hot_lo + (k * stride) % hot_size }
        else                        { op = "I"; isn = base_rows + (k - n_upd - n_del) + 1 }
        # existing rows keep their created date (= their partition); new rows
        # are created on the change day and land in a new partition
        cday = (isn <= base_rows) ? start_day + int((isn - 1) * span_days / base_rows) : change_day
        # commit times grow with k: every change has its own timestamp
        ts = fmt_date(change_day) sprintf(" %02d:%02d:%02d.%06d", int(k / 3600000000) % 24, int(k / 60000000) % 60, int(k / 1000000) % 60, k % 1000000)

        rec = sprintf("\"created\":\"%s\",\"txn_ref\":\"TX%016d\",\"acct_isn\":%d,\"card_isn\":%s,\"type\":\"%s\",\"amount\":%.2f,\"currency\":\"%s\",\"booking_date\":\"%s\"",
                      fmt_date(cday), isn, 1 + int(rand() * n_acct),
                      (rand() < 0.40 ? "null" : 1 + int(rand() * n_card)),
                      TYPE[1 + int(rand() * 6)], -8000 + rand() * 16000, CURR[1 + int(rand() * 5)],
                      fmt_date(cday + int(rand() * 3)))
        if (rand() >= 0.30)
            rec = rec sprintf(",\"merchant\":{\"mcc\":%d,\"name\":\"%s\",\"city\":\"%s\",\"country\":\"%s\"}",
                              1000 + int(rand() * 9000), MNAME[1 + int(rand() * 8)], MCITY[1 + int(rand() * 8)], MCTRY[1 + int(rand() * 8)])
        r = rand()
        if (r >= 0.85)      rec = rec sprintf(",\"tag\":[\"%s\",\"%s\"]", TAG[1 + int(rand() * 5)], TAG[1 + int(rand() * 5)])
        else if (r >= 0.50) rec = rec sprintf(",\"tag\":[\"%s\"]", TAG[1 + int(rand() * 5)])
        rec = rec ",\"reversal\":" (rand() < 0.03 ? "true" : "false")

        printf "{\"hdr\":{\"isn\":%d,\"op\":\"%s\",\"ts\":\"%s\",\"batch\":1},\"rec\":{%s}}\n", isn, op, ts, rec
    }
}

function coprime_stride(n,    s) {
    if (n <= 2) return 1
    for (s = int(n * 0.618) + 1; s > 1; s--) if (gcd(s, n) == 1) return s
    return 1
}
function gcd(a, b,    t) { while (b) { t = a % b; a = b; b = t } return a }

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
