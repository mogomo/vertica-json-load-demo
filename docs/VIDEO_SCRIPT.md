# Video: JSON upserts into a 1-billion-row Vertica table

An animated video (Manim) with a recorded voice-over and screenshots of real runs.

## Narration

Read it straight through, without pauses; the scenes are cut to the audio afterwards. About
1,750 words: 11 to 12 minutes at a calm, friendly pace. Numbers are written as they are spoken.

### The challenge

> Hello, and welcome. Today we're going to solve a very practical problem: how do you bring
> large batches of JSON changes into a very large Vertica table, as fast as possible, and
> prove that the result is correct?
>
> Here's the situation. A mainframe system, running the ADABAS database, publishes every change
> it makes as JSON. Every batch carries one million records. Half of them update rows that
> already exist, and the other half are brand-new rows. And the target is big: a fact table
> with one billion rows. So every batch is what we call an upsert: update what's already
> there, insert what's new. Our goal is to do the whole job, from the first byte of JSON to the
> last committed row, in seconds, not minutes. And we want to run it again and again, with
> exactly the same result.
>
> So why is this a challenge at all? Vertica is a columnar database. It's built to scan and to
> append data at enormous speed, but it never changes a row in place. Under the hood, an update
> is a delete plus an insert. And every deleted row leaves a small marker behind, called a
> delete vector, until the database cleans it up in the background. So the real question is not
> just how to update. It's how to update in a way that plays to Vertica's strengths. Let's look
> at three ways to do it, one by one.

### Method 1: the insert-only upsert

> Method one is the insert-only upsert. The idea is beautifully simple: never update at all.
> Every change, whether it's an update or a new row, is appended to a journal table, together
> with the time of the change. So the journal holds every version of every row.
>
> Now, you might ask: if there are several versions of a row, how do readers find the latest
> one? This is where a special Vertica feature comes in: a Top-K live aggregate projection.
> Think of a projection as a second physical copy of a table that Vertica keeps up to date for
> you. This particular one keeps only one row per key: the row with the newest timestamp. In
> SQL it's simply LIMIT 1 OVER, partition by the key, order by the change time, descending.
> Vertica maintains it while the data is being loaded, so it's always ready. Readers query a
> view, the optimizer sends that query to the projection, and they see exactly one current row
> per key.
>
> What do we gain? Applying a batch costs the same as loading it. There's no join, no delete,
> and no delete vectors. And as a bonus, we get the full history of every row, for free. What
> do we pay? Roughly twice the storage, because of that second copy. A query that reads all the
> current rows has to pick the latest version on the fly, so full scans are slower. And from
> time to time, we compact the journal.

### Method 2: staging table and partition swap

> Method two takes a completely different approach: the fact table is never changed row by row.
> The table is partitioned by month, using a creation date that never changes. That's
> important: it guarantees that an updated row always stays in the same partition. Think of
> the partitions as drawers in a cabinet.
>
> First, we load the batch into a staging table. Then we ask a simple question: which drawers
> do these changes touch? In our case, the last two months, plus a new month for the new rows.
> We rebuild only those drawers, in a side table: the rows that didn't change, plus the new
> versions of the rows that did. And then, with one command, swap partitions between tables,
> Vertica exchanges the old drawers for the new ones.
>
> That swap is a catalog operation. It takes milliseconds, it's atomic, and readers see either
> the old data or the new data, never a mix. The fact table never gets a single delete vector,
> so there's nothing to clean up later. The price is that we rewrite every row of the touched
> drawers, not just the rows that changed. In our case that's fifty-six million rows to apply
> one million changes. To give this method its best shot, we rebuild those partitions with
> eleven sessions working in parallel.

### Method 3: the optimized MERGE

> Method three is the optimized MERGE. MERGE is the classic upsert statement: match each
> incoming row to the table by its key, update it when it exists, insert it when it doesn't.
> We load the batch into a delta table that has the same sort order and the same distribution
> as the target, and we run a single MERGE.
>
> The key word here is optimized. Vertica has a fast path for MERGE, and it takes it when three
> rules are met. One: the key is declared as a primary key. Don't worry, it isn't enforced, so
> it costs nothing at load time. Two: the update and the insert list every column of the
> table. And three: both use the same values. When those rules hold, Vertica runs the MERGE as
> a simple delete plus an insert, over a merge join of two sorted inputs, instead of the
> generic and much slower plan. And you don't have to take my word for it: the script prints
> the plan for you. The price of this method: every updated row leaves a delete vector, which
> Vertica purges later, in the background.

### Phase 1: the results

> Before we look at the numbers, a word about fairness. Every method starts from exactly the
> same billion rows. The script resets each table with COPY TABLE, which takes a few
> milliseconds, because the copy shares the storage of the original. And the clock covers
> everything: parsing the JSON, loading it, and applying the changes.
>
> Here are the results of phase one, one million changes into one billion rows, averaged over
> three runs. The insert-only upsert: two point one seconds. The optimized MERGE: two point two
> seconds, and the MERGE itself takes less than one second, on a billion-row table. The
> partition swap: about sixteen seconds, because it rewrites fifty-six million rows. The three
> runs are almost identical, and all three methods produce exactly the same data, down to the
> last of more than one billion rows, checked with a count and a checksum.

### Phase 2: mixed JSON files

> Now, real CDC files are messier. They don't contain one table. They mix the changes of many
> files. So in phases two and three, we add nine more ADABAS files: customers, accounts, cards,
> loans, payments, policies, claims, employees and vehicles, with sixty million rows each,
> next to our billion-row table. Every table receives one million changes: ten million changes
> in total. And based on phase one, we use the optimized MERGE: just as fast as the upsert, with
> half the storage.
>
> In phase two, each line of JSON is one record of one table, and every file mixes all ten
> tables. The tempting idea is to run ten COPY statements, one per table, and let each one
> reject what isn't its own. But parsing JSON is the expensive part, and that would parse every
> record ten times. And COPY can only add rows; it can't update. So we parse once. One COPY reads
> all the files in parallel into one wide staging table, with the columns of all ten tables
> side by side. Each record fills only its own columns, and the empty ones cost almost nothing
> in a columnar database. Then ten MERGE statements run in parallel, each one reading its own
> table's rows straight from the staging table.
>
> The result: thirty seconds to parse ten million JSON records, and two and a half seconds for
> all ten MERGEs together. Thirty-two and a half seconds in total.

### Phase 3: nested documents

> Phase three uses the very same ten million changes, but in a different shape. Each line is
> now one mainframe transaction: a nested document that carries an array of changed records
> for each file. Up to five levels deep. Again, we parse once. The parser flattens the arrays,
> so the second customer record of a document arrives with a key like customer, dot one, dot
> rec, dot name. Our staging table simply has two slots of columns for each table, named
> exactly like those keys, and each MERGE reads its table as the union of its slots.
>
> The result: twenty-one seconds to parse, five and a half seconds to merge, twenty-six and a
> half seconds in total. And here's an interesting lesson: the nested documents load faster
> than the same data as flat lines. The same records arrive in one tenth of the rows, so the
> parser does far less work per record.
>
> What about parallelism? Ten MERGEs at a time, or five at a time, finish within about half a
> second of each other, and both beat running the tables one after the other by six to nine
> seconds. And in every run, every table passes its check: the rows that changed in the table
> match the JSON records exactly.

### What we learned

> So, what did we learn? First: when a batch is a small slice of a big table, the optimized
> MERGE and the insert-only upsert are equally fast, and most of the time goes into parsing
> the JSON. Second: choose the optimized MERGE when you want one copy of the data and you can
> meet its three rules. Choose the insert-only upsert when MERGE can't be optimized, or when
> you need the history of every row. And choose the partition swap when a batch rewrites most
> of a few small partitions, or when readers must see a whole batch at once. And third: when
> one file feeds many tables, parse it once, and then fan out inside the database, in
> parallel.
>
> The scripts, the SQL and every result are on GitHub. Clone it, run it on your own Vertica,
> and measure for yourself. Thanks for watching.

## Scenes

Cut the scenes to the recorded audio.

| # | Narration starts with | On screen (Manim) | Screenshot |
|---|---|---|---|
| 1 | "Hello, and welcome…" | title card | – |
| 2 | "Here's the situation…" | mainframe icon → JSON lines flowing → a table labelled **1,000,000,000 rows**; counter "1,000,000 changes: 50 % update · 50 % insert" | – |
| 3 | "So why is this a challenge…" | a column store: an "update" splits into delete + insert; a small delete-vector marker appears | – |
| 4 | "Method one…" | rows appended to a journal with timestamps; a second "projection" copy keeps only the newest row per key (LIMIT 1 OVER …) | the Top-K projection DDL and the version history panel of `./phase1.sh` |
| 5 | "Method two…" | the table as a cabinet of monthly drawers; 3 drawers light up, are rebuilt aside, then swap in one move | the "touched partitions" line and the swap step |
| 6 | "Method three…" | MERGE as matching two sorted lists; the three rules appear as check marks; plan: DELETE + INSERT over merge join | the EXPLAIN panel ("optimized MERGE") |
| 7 | "Before we look at the numbers…" | COPY_TABLE: a new table pointing at the same storage blocks; "0.04 s, 0 bytes" | a "reset (COPY_TABLE) — not timed" line |
| 8 | "Here are the results of phase one…" | bar chart, stacked parse + apply: 2.12 s, 2.21 s, 15.89 s; checksums match | the RESULTS table of `./phase1.sh --runs 3` |
| 9 | "Now, real CDC files are messier…" | 10 tables (one big, nine smaller); a mixed JSON stream with colored records | – |
| 10 | "In phase two…" | crossed out: 10 parsers over the same files; then one parser → wide table → 10 MERGE arrows in parallel | the phase 2 COPY step and the RESULTS table of `./phase2.sh` |
| 11 | "Phase three…" | a nested document unfolding into arrays; flattened keys `customer.1.rec.name.first`; two slots per table | the phase 3 sample document and the RESULTS table of `./phase3.sh` |
| 12 | "What about parallelism?" | three bars: 10 / 5 / 1 at a time; check marks on 10 tables | the "check" line |
| 13 | "So, what did we learn?" | decision matrix: upsert / swap / MERGE; "parse once, fan out" | – |
| 14 | "The scripts…" | repository URL, end card | – |

## Recording the terminal (for the screenshots)

Use a terminal at least 120 columns wide, a dark theme and a large font.

```bash
./generate.sh                                   # off camera (~12 min)
./phase1.sh --pause                             # one run, Enter before every step: WHAT / WHY / SQL
./phase1.sh --runs 3                            # the phase 1 results table
./generate_multi.sh                             # off camera (~2.5 min); shows both JSON shapes
./phase2.sh --pause                             # the phase 2 steps
./phase2.sh --parallel 10,5,1 --runs 3          # the phase 2 results table
./phase3.sh --parallel 10,5,1 --runs 3          # the phase 3 results table
```
