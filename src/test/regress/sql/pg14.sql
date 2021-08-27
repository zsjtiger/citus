SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 13 AS server_version_above_thirteen
\gset
\if :server_version_above_thirteen
\else
\q
\endif
CREATE SCHEMA pg14test;
SET search_path TO "pg14test";
SET citus.next_shard_id TO 980000;
SET citus.shard_count TO 2;

-- test the new vacuum option, process_toast
CREATE TABLE t1 (a int);
SELECT create_distributed_table('t1','a');
SET citus.log_remote_commands TO ON;
VACUUM (FULL) t1;
VACUUM (FULL, PROCESS_TOAST) t1;
VACUUM (FULL, PROCESS_TOAST true) t1;
VACUUM (FULL, PROCESS_TOAST false) t1;
SET client_min_messages TO WARNING;
-- error out in case of ALTER TABLE .. DETACH PARTITION .. CONCURRENTLY/FINALIZE
-- only if it's a distributed partitioned table
CREATE TABLE par (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE par_1 PARTITION OF par FOR VALUES FROM (1) TO (4);
CREATE TABLE par_2 PARTITION OF par FOR VALUES FROM (5) TO (8);
-- works as it's not distributed
ALTER TABLE par DETACH PARTITION par_1 CONCURRENTLY;
-- errors out
SELECT create_distributed_table('par','a');
ALTER TABLE par DETACH PARTITION par_2 CONCURRENTLY;
ALTER TABLE par DETACH PARTITION par_2 FINALIZE;

DROP SCHEMA pg14test CASCADE;
