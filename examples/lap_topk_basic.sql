-- =============================================================================
--  lap_topk_basic.sql — the Top-K LAP technique in 30 lines
--  Run:  vsql -f examples/lap_topk_basic.sql
-- =============================================================================
--  Instead of UPDATE/DELETE, INSERT a new version of the row. A Top-K Live
--  Aggregate Projection keeps the latest version per key; the base (anchor)
--  table keeps the full history (insert/update/delete time of every version).
-- =============================================================================
DROP TABLE IF EXISTS my_table CASCADE;

CREATE TABLE my_table (
    subject_id  INT,
    subject     VARCHAR(10),
    insert_date DATETIME DEFAULT SYSDATE()
)
ORDER BY subject_id;

-- select list order for a Top-K projection: PARTITION BY column(s),
-- then ORDER BY column(s), then the remaining columns
CREATE PROJECTION my_topk_lap (subject_id, insert_date, subject)
AS SELECT subject_id, insert_date, subject
     FROM my_table
    LIMIT 1 OVER (PARTITION BY subject_id ORDER BY insert_date DESC);

COPY my_table (subject_id, subject) FROM STDIN DELIMITER ',' ABORT ON ERROR;
1,A
2,B
3,C
4,D
\.

\echo 'To "update" B we just INSERT the new B with the same subject_id:'
INSERT INTO my_table (subject_id, subject) VALUES (2, 'New B');
COMMIT;

\echo 'Latest version per key (answered from the LAP):'
SELECT subject_id, subject, insert_date FROM my_topk_lap ORDER BY 1;

\echo 'Full history in the anchor table:'
SELECT subject_id, subject, insert_date FROM my_table ORDER BY 1, 3 DESC;

\echo 'Purge (every few months): keep only the latest version'
-- Step 1: new table with the last version of the data
CREATE TABLE my_table_new LIKE my_table;
INSERT INTO my_table_new SELECT subject_id, subject, insert_date FROM my_topk_lap;
COMMIT;
-- Steps 2+3: atomic rename (readers never see the table missing), then drop
-- the old table with its projections and LAP
ALTER TABLE my_table, my_table_new RENAME TO my_table_old, my_table;
DROP TABLE my_table_old CASCADE;
-- Step 4: re-create the LAP on the new base table
CREATE PROJECTION my_topk_lap (subject_id, insert_date, subject)
AS SELECT subject_id, insert_date, subject
     FROM my_table
    LIMIT 1 OVER (PARTITION BY subject_id ORDER BY insert_date DESC);
SELECT REFRESH('my_table');

SELECT subject_id, subject, insert_date FROM my_table ORDER BY 1;
DROP TABLE my_table CASCADE;
