# =============================================================================
#  gen_docs.awk — the same change records in the two JSON shapes of phases 2
#  and 3
# =============================================================================
#  Input: one file per table (in the order of the "tables" variable), each a
#  slice of that table's change records as written by gen_json.awk:
#      {"hdr":{"isn":…,"op":…,"ts":…,"batch":1},"rec":{…}}
#  Records of all tables are grouped into ADABAS transactions (ET): document
#  d holds P[(d + table#) % 4] records of each table, P = 0, 1, 1, 2. Over any
#  4 consecutive documents every table gets 4 records, so documents d0 … d1-1
#  (d0, d1 multiples of 4) use exactly records d0 … d1-1 of every table.
#
#  Output:
#    phase2 file  one record per line, table named in the header, record under
#                 a key with the table's name:
#                 {"hdr":{"file":"customer","isn":…},"customer":{…}}
#    phase3 file  one document (transaction) per line, one array per table:
#                 {"et_id":…,"et_ts":"…","customer":[{"hdr":…,"rec":…}],"account":[],…}
#
#  Variables (-v): tables (space separated), d0, d1, phase2, phase3, et_day
# =============================================================================
BEGIN {
    nt = split(tables, T, " ")
    P[0] = 0; P[1] = 1; P[2] = 1; P[3] = 2
    for (d = d0; d < d1; d++) {
        doc = sprintf("{\"et_id\":%d,\"et_ts\":\"%s %02d:%02d:%02d\"", d + 1, et_day, int(d / 3600) % 24, int(d / 60) % 60, d % 60)
        for (i = 1; i <= nt; i++) {
            t = T[i]; n = P[(d + i - 1) % 4]; arr = ""
            for (j = 0; j < n; j++) {
                if ((getline line < ARGV[i]) <= 0) { print "gen_docs.awk: " ARGV[i] " ended early" > "/dev/stderr"; exit 2 }
                arr = arr (j ? "," : "") line
                flat = line
                sub(/^\{"hdr":\{/, "{\"hdr\":{\"file\":\"" t "\",", flat)
                sub(/,"rec":\{/, ",\"" t "\":{", flat)
                print flat > phase2
            }
            doc = doc ",\"" t "\":[" arr "]"
        }
        print doc "}" > phase3
    }
    exit 0
}
