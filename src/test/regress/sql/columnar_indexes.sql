--
-- Testing indexes on on columnar tables.
--

CREATE SCHEMA columnar_indexes;
SET search_path tO columnar_indexes, public;

--
-- create index with the concurrent option. We should
-- error out during index creation.
-- https://github.com/citusdata/citus/issues/4599
--
create table t(a int, b int) using columnar;
create index CONCURRENTLY t_idx on t(a, b);
\d t
explain insert into t values (1, 2);
insert into t values (1, 2);
SELECT * FROM t;

-- create index without the concurrent option. We should
-- error out during index creation.
create index t_idx on t(a, b);
\d t
explain insert into t values (1, 2);
insert into t values (3, 4);
SELECT * FROM t;

-- make sure that we test index scan
set columnar.enable_custom_scan to 'off';
set enable_seqscan to off;

CREATE table columnar_table (a INT, b int) USING columnar;
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(0, 16000) i;

-- unique --
BEGIN;
  INSERT INTO columnar_table VALUES (100000000);
  SAVEPOINT s1;
  -- errors out due to unflushed data in upper transaction
  CREATE UNIQUE INDEX ON columnar_table (a);
ROLLBACK;

CREATE UNIQUE INDEX ON columnar_table (a);

BEGIN;
  INSERT INTO columnar_table VALUES (16050);
  SAVEPOINT s1;
  -- index scan errors out due to unflushed data in upper transaction
  SELECT a FROM columnar_table WHERE a = 16050;
ROLLBACK;

EXPLAIN (COSTS OFF) SELECT * FROM columnar_table WHERE a=6456;
EXPLAIN (COSTS OFF) SELECT a FROM columnar_table WHERE a=6456;
SELECT (SELECT a FROM columnar_table WHERE a=6456 limit 1)=6456;
SELECT (SELECT b FROM columnar_table WHERE a=6456 limit 1)=6456*2;

-- even if a=16050 doesn't exist, we try to insert it twice so this should error out
INSERT INTO columnar_table VALUES (16050), (16050);

-- should work
INSERT INTO columnar_table VALUES (16050);

-- check edge cases around stripe boundaries, error out
INSERT INTO columnar_table VALUES (16050);
INSERT INTO columnar_table VALUES (15999);

DROP INDEX columnar_table_a_idx;

CREATE TABLE partial_unique_idx_test (a INT, b INT) USING columnar;
CREATE UNIQUE INDEX ON partial_unique_idx_test (a)
WHERE b > 500;

-- should work since b =< 500 and our partial index doesn't check this interval
INSERT INTO partial_unique_idx_test VALUES (1, 2), (1, 2);

-- should work since our partial index wouldn't cover the tuples that we inserted above
INSERT INTO partial_unique_idx_test VALUES (1, 800);

INSERT INTO partial_unique_idx_test VALUES (4, 600);

-- should error out due to (4, 600)
INSERT INTO partial_unique_idx_test VALUES (4, 700);

-- btree --
CREATE INDEX ON columnar_table (a);
SELECT (SELECT SUM(b) FROM columnar_table WHERE a>700 and a<965)=439560;

CREATE INDEX ON columnar_table (b)
WHERE (b > 30000 AND b < 33000);

-- partial index should be smaller than the non-partial index
SELECT pg_total_relation_size('columnar_table_b_idx') <
       pg_total_relation_size('columnar_table_a_idx');

-- can't use index scan due to partial index boundaries
EXPLAIN (COSTS OFF) SELECT b FROM columnar_table WHERE b = 30000;
-- can use index scan
EXPLAIN (COSTS OFF) SELECT b FROM columnar_table WHERE b = 30001;

-- some more rows
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(16000, 17000) i;

DROP INDEX columnar_table_a_idx;
TRUNCATE columnar_table;

-- pkey --
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(16000, 16499) i;
ALTER TABLE columnar_table ADD PRIMARY KEY (a);
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(16500, 17000) i;

BEGIN;
  INSERT INTO columnar_table (a) SELECT 1;
ROLLBACK;

-- should work
INSERT INTO columnar_table (a) SELECT 1;

-- error out
INSERT INTO columnar_table VALUES (16100), (16101);
INSERT INTO columnar_table VALUES (16999);

TRUNCATE columnar_table;
INSERT INTO columnar_table (a, b) SELECT i,i*2 FROM generate_series(1, 160000) i;
SELECT (SELECT b FROM columnar_table WHERE a = 150000)=300000;

TRUNCATE columnar_table;
ALTER TABLE columnar_table DROP CONSTRAINT columnar_table_pkey;

-- hash --
INSERT INTO columnar_table (a, b) SELECT i*2,i FROM generate_series(1, 8000) i;
CREATE INDEX hash_idx ON columnar_table USING HASH (b);

BEGIN;
  INSERT INTO columnar_table (a, b) SELECT i*3,i FROM generate_series(1, 8000) i;
ROLLBACK;

INSERT INTO columnar_table (a, b) SELECT i*4,i FROM generate_series(1, 8000) i;

SELECT SUM(a)=42000 FROM columnar_table WHERE b = 7000;

-- exclusion contraints --
CREATE TABLE exclusion_test (c1 INT,c2 INT, c3 INT, c4 BOX,
  EXCLUDE USING btree (c1 WITH =) INCLUDE(c3,c4) WHERE (c1 < 10)) USING columnar;

-- error out "c1" is "1" for all rows to be inserted
INSERT INTO exclusion_test SELECT 1, 2, 3*x, BOX('4,4,4,4') FROM generate_series(1,3) AS x;

BEGIN;
  INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(1,3) AS x;
ROLLBACK;

-- should work
INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(1,3) AS x;

INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(10,15) AS x;

-- should work due to "where" clause in exclusion constraint
INSERT INTO exclusion_test SELECT x, 2, 3*x, BOX('4,4,4,4') FROM generate_series(10,15) AS x;

-- gin --
CREATE TABLE testjsonb (
       j jsonb
) USING columnar;

INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;

CREATE INDEX jidx ON testjsonb USING GIN (j);

INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;
INSERT INTO testjsonb SELECT CAST('{"f1" : ' ||'"'|| i*4 ||'", ' || '"f2" : '||'"'|| i*10 ||'"}' AS JSON) FROM generate_series(1,1012) i;

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_indexes CASCADE;
