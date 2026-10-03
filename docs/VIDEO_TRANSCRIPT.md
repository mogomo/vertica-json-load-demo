# MERGE 1 Million Changes into a 1-Billion-Row Table in Under a Second

*Three ways to update data in Vertica*: the video transcript.

The narration of the video (13 minutes), which walks through this repository. Each section
starts with its time in the video.

Watch it on YouTube: https://youtu.be/qQx64DbZ01w

The numbers in the video come from the run recorded for it. 

## 0:00 Welcome

Hello, and welcome to this workshop. Today I'd like to share with you three cool ways to update
data in Vertica. Each one gets the best out of Vertica's columnar engine, and each one stays
simple: plain SQL, a few statements, no external tools.

First, what makes an update special in a columnar database. Then the three methods, one by one,
with the real syntax. And then we run them: 1 million JSON changes into a table of 1 billion
rows, and 10 million changes into 10 tables. Every number you'll see comes from a real recorded
run. Let's get started.


## 0:41 Updates in a columnar database

Vertica is a columnar database. Instead of storing a table row by row, it stores every column on
its own, sorted and compressed. That's why it scans billions of values in seconds, and loads so
fast.

But those files are never changed in place. So Vertica does something smarter: an update
becomes a delete plus an insert. And a delete doesn't remove anything right away: it records the
position of the old row in a small structure called a delete vector. Queries skip those
positions, and later, a background process called mergeout purges them for good.

So updates and deletes work perfectly well in Vertica. The way to make them fast is to play to
its strengths: work in large sets, append rather than change, join data that is already sorted,
and let the catalog do the heavy lifting. Each of our three methods does exactly that.


## 1:37 The use case: ADABAS changes as JSON

Before we run anything, let's look at the use case. Many organizations still run their core
systems on a mainframe, with an ADABAS database, and they want that data in Vertica, fresh, for
analytics. So every change on the mainframe is captured and published as JSON. That's change
data capture.

ADABAS keeps its records in numbered files, and every record has a permanent key, called the
ISN. A record can hold groups of fields, fields with several values, and periodic groups that
repeat, like an array of structures. In JSON, those become nested objects and arrays. Here's one
change record. A small header: the ISN, the operation, U for update or I for insert, the commit
time and the batch number. Then the record itself, with the merchant as a nested group, and the
tags as an array.

Our target is the ADABAS transactions file in Vertica: a fact table with 1 billion rows,
partitioned by month on the creation date. Every batch brings 1 million changes. Half update
existing rows, half are new rows. In other words: an upsert.

To keep it fair, every method starts from the same billion rows: the script resets the table
with `COPY_TABLE`, which takes milliseconds, because the copy shares the original's storage. And
the clock covers the whole job: parsing the JSON, loading it, and applying the changes.


## 3:08 The common first step: COPY

All three methods begin the same way, with Vertica's `COPY` statement. `COPY` reads the 22 JSON
files directly, `ON ANY NODE`, and `FJSONPARSER` turns each record into a row. Nested keys
simply become column names, like `rec.merchant.city`. With `flatten_arrays` set to true, every
array element gets its position in the name, like `rec.tag.0`.

Each file gets its own parse thread, so a single `COPY` puts every available core to work in
parallel. 1 million JSON records, parsed and loaded in a little over one second.


## 3:45 Method 1: the insert-only upsert

Method one is the insert-only upsert, and the idea is beautifully simple: never update at all.
Every change is appended to a journal table, together with its change time. So the journal keeps
every version of every row.

Readers want only the latest version, and here a special Vertica feature comes in: a Top-K live
aggregate projection, kept up to date by Vertica during every load. It keeps just one row per
key, and the syntax reads almost like English: `CREATE PROJECTION`, select the columns from the
journal, `LIMIT 1 OVER (PARTITION BY isn ORDER BY change_ts DESC)`. One row per ISN: the newest.

Readers query a simple view, answered from the projection; `EXPLAIN` even says
`TopK Optimized`. Look at ISN 950000001: the journal holds two versions, and the current view
returns one row, the update.

So applying a batch costs exactly the same as loading it. No join, no delete, no delete vectors,
and the full history of every row for free. The trade-offs: roughly twice the storage, slower
full scans of the current rows, and from time to time, you compact the journal.


## 5:04 Method 2: staging table and partition swap

Method two works with whole partitions. The creation date never changes, so an updated row
always stays in its monthly partition. Think of the partitions as drawers in a cabinet.

`COPY` loads the batch into a staging table, and one quick query asks: which drawers do these
changes touch? Here, three: November, December, and the new January. Then we rebuild only those
three drawers in a side table, with a plain `INSERT … SELECT`: the rows that didn't change, plus
the new versions of the rows that did. The rebuild is split by ISN range into 11 slices, running
in 11 parallel sessions.

And then comes the magic moment. One function call: `SWAP_PARTITIONS_BETWEEN_TABLES`. Vertica
exchanges the drawers in its catalog. It takes milliseconds, and it's atomic.

The fact table never gets a delete vector. The price: we rewrite every row of the touched
drawers, not just the changed ones. Here, that's 56 million rows, to apply 1 million changes.


## 6:11 Method 3: the optimized MERGE

Method three is the classic: `MERGE INTO` the target, `USING` the delta, `ON` the key.
`WHEN MATCHED THEN UPDATE`. `WHEN NOT MATCHED THEN INSERT`. One statement for the whole upsert,
from a delta table created `LIKE` the target.

The key word is optimized. Vertica takes a fast path when three simple rules hold. One: the join
key is declared as a primary key. Declared, not enforced, so it costs nothing at load time. Two:
the `UPDATE SET` and the `INSERT` list every column of the table. Three: both use the same source
values.

Then Vertica runs the `MERGE` as a plain delete plus an insert, over a merge join of two inputs
that are already sorted. You can see it in the plan: `DML DELETE`, `DML INSERT`, a merge join
with presorted inputs, and no generic `MERGE` operator. The only price: every updated row leaves
a delete vector, which mergeout purges later, in the background.


## 7:14 The three methods side by side

Now let's see them run, side by side. The same billion rows, the same 1 million JSON changes,
the same 22 files, the same machine. Three runs per method, averaged.

The insert-only upsert: 2.07 seconds for the whole job. The apply step costs zero, because the
load is the upsert.

The optimized MERGE: 2.13 seconds. 1.27 to parse and load, and the `MERGE` itself: 0.86
seconds. Less than one second, on a billion-row table.

The partition swap: 15.6 seconds. Most of that rebuilds 56 million rows; the swap itself takes
0.03 seconds.

Notice where the time goes. Parsing and loading the JSON costs about the same in all three
methods. The difference is the apply step: zero for the upsert, under one second for the
`MERGE`, and 14 seconds for the swap, because its cost follows the size of the touched
partitions, not the number of changes.

And beyond speed: the upsert and the swap leave no delete vectors, the `MERGE` leaves one per
updated row. The `MERGE` and the swap keep one copy of the data. The upsert keeps about two, plus
the full history.

After every run, the script checks the result. All three methods end with exactly the same
56,156,934 live rows in the touched range, with the same checksum. Three different methods, one
identical answer.


## 8:47 Phase 2: ten tables, one mixed JSON stream

Real change streams carry many files at once. So in phase two, we add nine more ADABAS files,
like customers, accounts, cards and loans, with 60 million rows each. Every table receives
1 million changes, 10 million in total, and we use the optimized `MERGE`: as fast as the upsert,
with one copy of the data.

Each JSON line is one record of one table, and every file mixes all ten tables. Ten `COPY`
statements, one per table, would parse every record ten times, and parsing is the expensive
part. So we parse once. One `COPY` reads all the files in parallel into one wide staging table,
with the columns of all ten tables side by side. Each record fills only its own columns; in a
columnar database, empty columns cost almost nothing.

Then we fan out: ten optimized `MERGE`s run in parallel, each reading its own rows straight from
the staging table, with a simple `WHERE file = 'customer'`.

The result: about 30 seconds to parse 10 million JSON records, and just 2.5 seconds for all ten
`MERGE`s together. 32.4 seconds in total.


## 10:02 Phase 3: nested documents

Phase three uses the very same 10 million changes, in a different shape. Each line is now one
mainframe transaction: a nested document, with an array of changed records for each file.

Again, we parse once. The parser flattens the arrays into keys: the second customer record of a
document arrives as `customer.1.rec.name.first`. So the staging table has two slots of columns
per table, named exactly like those keys, and the `COPY` doesn't even need a column list. Each
`MERGE` reads its table as a `UNION ALL` of its two slots. The plan uses a hash join here, but
it's still the optimized `MERGE`: a delete plus an insert.

The result: 20.3 seconds to parse, 5.6 to merge, 25.9 seconds in total. A nice lesson: the
nested documents load faster than the same data as flat lines, because the same records arrive
in one tenth of the rows.


## 11:03 Parallelism and checks

What about parallelism? Ten `MERGE`s at a time and five at a time finish within one second of
each other, and both beat one at a time by 6 to 9 seconds.

And in every run, every table passes its check: its 1 million changed rows match the JSON
records exactly, by count and by a checksum of every column.


## 11:26 Choosing your method

So, which method should you choose? Vertica gives you real options.

Choose the optimized `MERGE` when you want one copy of the data and you can meet its three simple
rules. It's the most familiar SQL, and on a billion rows it applies a million changes in under a
second.

Choose the insert-only upsert when you need the history of every row, or when a `MERGE` can't be
optimized. The load is the upsert.

Choose the partition swap when a batch rewrites a large part of a few partitions, or when
readers must see a whole batch appear at once.

And when one file feeds many tables: parse it once, and fan out inside the database, in
parallel.


## 12:09 Closing

That's it: three cool ways to update data in Vertica, all in plain SQL, all measured on real
data. The scripts, the SQL and every result are on GitHub, in the repository
[vertica-json-load-demo](https://github.com/mogomo/vertica-json-load-demo). Clone it, run it on
your own Vertica, and measure for yourself.


## 12:28 Final words

Let's put it all together. 1 million JSON changes, into a table of 1 billion rows. Parsing and
loading 1 million JSON records takes 1.22 seconds. The optimized `MERGE` itself: under one
second. The partition swap itself: 0.03 seconds. And copying a billion-row table with
`COPY_TABLE`: 0.04 seconds. And remember: all of this ran on a single laptop. On a decent
Vertica cluster, every node parses, loads and merges its share, so it runs even faster. Thanks
for watching.
