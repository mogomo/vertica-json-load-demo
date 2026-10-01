# Video: 1 million JSON changes into 1 billion rows, three ways

A short animated video (Manim) with a recorded voice-over and screenshots of a real run.
Target length: about 6½ minutes.

## Narration

Read it straight through, without pauses; the scenes are cut to the audio afterwards.
About 900 words: 6 to 6½ minutes at a calm pace.

> A mainframe ADABAS system sends its changes as hierarchical JSON. Every batch holds one
> million records: half of them update rows that already exist, and half of them are new
> rows. Our target is a fact table in Vertica with one billion rows. The question is simple:
> what is the fastest way to get those million JSON rows into the billion-row table,
> including the time it takes to parse the JSON?
>
> We compare three methods. The first is an insert-only upsert: we never update, we only
> append, and a Top-K live aggregate projection always serves the newest version of every
> row. The second is a staging table with partition swap: we rebuild only the partitions
> that the changes touch, and publish them with one atomic swap. The third is an optimized
> MERGE: we load the changes into a delta table and apply them with a single MERGE statement
> that Vertica can run through its fast plan.
>
> The demo is two bash scripts with the SQL inside. The first script, generate dot s h,
> builds the data. It creates the one-billion-row table inside Vertica with SQL, eleven
> parallel sessions, one monthly partition per statement, in under four minutes. It copies
> the same rows into a journal with its Top-K projection, and it writes the one million
> changes as JSON files in the demo folder. Each record has a CDC header, with the ISN, the
> operation and the commit time, and the ADABAS record itself, with a group and a
> multiple-value field.
>
> The second script, apply dot s h, runs the three methods. Before each method, it resets the
> table with COPY TABLE. That is a catalog operation: the copy shares the storage of the
> original, so a billion rows are reset in a few milliseconds, without using any disk. That
> is why the demo can run again and again, and every run starts from exactly the same data.
>
> Every method begins with the same COPY statement. The JSON parser flattens the nested
> keys, filler columns map them to the table columns, and twenty-two files are parsed in
> parallel. The timer runs from the first byte of JSON to the last committed row.
>
> Here are the results, the average of three runs. The insert-only upsert: two point one
> seconds. The COPY is the whole job; there is no apply step at all. The optimized MERGE: two
> point two seconds. One point three seconds to parse and load, and less than one second for
> the MERGE itself, on a billion-row table. The plan shows a delete and an insert over a
> presorted merge join, which is the optimized path. The partition swap: about sixteen
> seconds. It must rewrite every row of the touched partitions, fifty-six million rows to
> apply one million changes, even with eleven sessions working in parallel.
>
> And the results are correct. After every method, the script counts the current rows and
> computes a checksum. All three methods produce exactly the same data, down to the last of
> one billion and five hundred thousand rows.
>
> Now let's scale out. A real offload has more than one file. So we add nine more ADABAS
> files, customers, accounts, cards, loans, payments, policies, claims, employees and
> vehicles, with sixty million rows each, next to the billion-row transaction table. Every one
> of the ten tables receives one million multi-level JSON changes: groups, multiple-value
> fields, and periodic groups, which are arrays of objects. That is ten million changes in
> all. Each table gets its own pipeline: a COPY that parses its JSON into a delta table, and an
> optimized MERGE. And because the tables are independent, the pipelines can run side by side.
>
> One table after the other, the ten tables take twenty-eight seconds. All ten in parallel:
> twelve point two seconds, more than twice as fast. Five at a time: twelve point nine
> seconds, almost the same. Why so close? Because parsing JSON is the real work, and each COPY
> already parses its files on many threads. With five tables at once, all twenty-two cores are
> busy, and the machine moves about eight hundred thousand JSON rows per second, whichever way
> we split the work. Ten at a time is not too heavy; five at a time gives the same throughput
> with half the sessions. And every table passes its check: the rows changed in the table match
> the JSON rows exactly.
>
> So, which method should you choose? When the changes are a small slice of a big table,
> the optimized MERGE and the insert-only upsert are equally fast, and most of the time is
> spent parsing the JSON. Choose MERGE when you want one copy of the data and you can meet
> its three rules: a declared key, every column, and the same values. Choose the upsert when
> MERGE can't be optimized, or when you need the history of every row; it costs twice the
> storage, and reading all current rows takes longer. And choose the partition swap when a
> batch rewrites most of a few small partitions, or when readers must see a whole batch at
> once, with no delete vectors left behind.
>
> The scripts, the SQL and the results are on GitHub. Clone it, run it on your own Vertica,
> and measure for yourself.

## Scenes

The timings in brackets are approximate; cut them to the recorded audio.

| # | Narration starts with | On screen (Manim) | Screenshot |
|---|---|---|---|
| 1 | "A mainframe ADABAS system…" | mainframe icon → JSON lines flowing → a table labelled **1,000,000,000 rows**; counter "1,000,000 changes: 50 % update, 50 % insert" | – |
| 2 | "We compare three methods…" | three columns appear one by one: **Upsert (journal + Top-K LAP)**, **Staging + partition SWAP**, **Optimized MERGE**, each with a 3-box flow diagram | – |
| 3 | "The demo is two bash scripts…" | `generate.sh` box: 36 partition blocks filling in parallel (11 lanes); a JSON record expands into its tree (hdr / rec / merchant / tag) | end of `./generate.sh` (READY panel) |
| 4 | "The second script, apply dot s h…" | `COPY_TABLE`: a table "copies" by pointing at the same storage blocks; label "0.04 s, 0 bytes" | step "Reset … (not timed)" |
| 5 | "Every method begins with the same COPY…" | 22 files → 22 parser threads → one table; stopwatch starts | the COPY SQL with FILLER columns |
| 6 | "Here are the results…" | bar chart builds bar by bar: 2.12 s, 2.21 s, 15.89 s (parse+load vs apply stacked) | the RESULTS table of `./apply.sh --runs 3` |
| 7 | "And the results are correct…" | three checksums slide together and match; "1,000,500,000 rows ✔" | the "check of run" panel |
| 7b | "Now let's scale out…" | 10 table icons (one big, nine smaller) each fed by its own JSON stream; a race: 10 bars in parallel vs 10 bars in a row; wall-clock 28.2 s → 12.2 s, 5 at a time 12.9 s; CPU meter at 100 % | the RESULTS table of `./apply_multi.sh --parallel 10,5,1 --runs 3` |
| 8 | "So, which method should you choose?" | decision matrix: best when / cost grows with / storage / history | – |
| 9 | "The scripts, the SQL and the results…" | repository URL, title card | – |

## Recording the terminal (for the screenshots)

Use a terminal at least 120 columns wide, a dark theme and a large font.

```bash
./generate.sh                        # off camera (~12 min), or show the end of its output
./apply.sh --pause                   # one run, Enter before every step: WHAT / WHY / SQL panels
./apply.sh --runs 3                  # the results table
./generate_multi.sh                  # off camera (~2.5 min)
./apply_multi.sh --parallel 10,5,1 --runs 3   # the 10-table results table
```
