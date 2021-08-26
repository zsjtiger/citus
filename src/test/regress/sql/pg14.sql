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
DROP SCHEMA pg14test CASCADE;
