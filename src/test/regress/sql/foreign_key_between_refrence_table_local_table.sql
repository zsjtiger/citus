CREATE SCHEMA fkey_reference_local_table;
SET search_path TO 'fkey_reference_local_table';

--- ALTER TABLE commands defining foreign key constraint between local tables and reference tables ---

-- create test tables

CREATE TABLE local_table(l1 int);
CREATE TABLE reference_table(r1 int primary key);
SELECT create_reference_table('reference_table');

-- foreign key from local table to reference table --

-- this should fail as reference table does not have a placement in coordinator
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- replicate reference table to coordinator
SELECT master_add_node('localhost', :master_port, groupId => 0);

-- we do support ALTER TABLE ADD CONSTRAINT foreign key from a local table to a
-- reference table within the transaction block
BEGIN;
  ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);
ROLLBACK;

-- we support ON DELETE CASCADE behaviour in "ALTER TABLE ADD foreign_key local_table
-- (to reference_table) commands
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE CASCADE;

-- show that ON DELETE CASCADE works
INSERT INTO reference_table VALUES (11);
INSERT INTO local_table VALUES (11);
DELETE FROM reference_table WHERE r1=11;
SELECT count(*) FROM local_table;

-- show that we support DROP foreign key constraint
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;

-- we support ON UPDATE CASCADE behaviour in "ALTER TABLE ADD foreign_key local_table
-- (to reference table)" commands
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE CASCADE;

-- show that ON UPDATE CASCADE works
INSERT INTO reference_table VALUES (12);
INSERT INTO local_table VALUES (12);
UPDATE reference_table SET r1=13 WHERE r1=12;
SELECT * FROM local_table ORDER BY l1;

-- DROP foreign_key constraint for next commands
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;

-- show that we are checking for foreign key constraint while defining

INSERT INTO local_table VALUES (2);

-- this should fail
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

INSERT INTO reference_table VALUES (2);

-- this should work
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- show that we are checking for foreign key constraint after defining

-- this should fail
INSERT INTO local_table VALUES (1);

INSERT INTO reference_table VALUES (1);

-- this should work
INSERT INTO local_table VALUES (1);

-- we do support ALTER TABLE DROP CONSTRAINT foreign key from a local table to a
-- reference table within the transaction block
BEGIN;
  ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;
ROLLBACK;

-- show that we do not allow removing coordinator when we have a foreign_key constraint
-- between a coordinator local table and a reference table
SELECT master_remove_node('localhost', :master_port);

-- show that DROP table without should error out without cascade
DROP TABLE reference_table;

-- DROP them at once
DROP TABLE reference_table CASCADE;

-- create one reference table and one distributed table for next tests
CREATE TABLE reference_table(r1 int primary key);
SELECT create_reference_table('reference_table');
CREATE TABLE distributed_table(d1 int primary key);
SELECT create_distributed_table('distributed_table', 'd1');

-- chain the tables's foreign key constraints to each other (local -> reference -> distributed)
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_dist FOREIGN KEY(r1) REFERENCES distributed_table(d1);

INSERT INTO distributed_table VALUES (41);
INSERT INTO reference_table VALUES (41);
-- this should work
INSERT INTO local_table VALUES (41);

-- below should fail
DROP TABLE reference_table;

-- below test if we handle the foreign key dependencies properly when issueing DROP command
-- (witohut deadlocks and with no weird errors etc.)
DROP TABLE local_table, reference_table, distributed_table;

-- create test tables

CREATE TABLE local_table(l1 int primary key);
CREATE TABLE reference_table(r1 int);
SELECT create_reference_table('reference_table');

-- remove master node from pg_dist_node
SELECT master_remove_node('localhost', :master_port);

-- foreign key from reference table to local table --

-- this should fail
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

-- we do support ALTER TABLE ADD CONSTRAINT foreign_key from a reference table
-- to a local table within the transaction block
BEGIN;
  ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);
ROLLBACK;

-- replicate reference table to coordinator

SELECT master_add_node('localhost', :master_port, groupId => 0);

-- show that we are checking for foreign key constraint while defining

INSERT INTO reference_table VALUES (3);

-- this should fail
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

INSERT INTO local_table VALUES (3);

-- we do not support ON DELETE/UPDATE CASCADE behaviour in "ALTER TABLE ADD foreign_key reference_table (to local_table)" commands
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1) ON DELETE CASCADE;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1) ON UPDATE CASCADE;

-- this should work
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

-- show that we are checking for foreign key constraint after defining

-- this should fail
INSERT INTO reference_table VALUES (4);

INSERT INTO local_table VALUES (4);

-- this should work
INSERT INTO reference_table VALUES (4);

-- we do support ALTER TABLE DROP CONSTRAINT foreign_key from a reference table
-- to a local table within a transaction block
BEGIN;
  ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;
COMMIt;

-- show that we do not allow removing coordinator when we have a foreign key constraint
-- between a coordinator local table and a reference table
SELECT master_remove_node('localhost', :master_port);

-- show that we support DROP CONSTRAINT
ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;

ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

-- show that DROP table errors out as expected
DROP TABLE local_table;

-- this should work
DROP TABLE local_table CASCADE;

-- DROP reference_table finally
DROP TABLE reference_table;

-- show that we can ADD foreign key constraint from/to a reference table that
-- needs to be escaped

CREATE TABLE local_table(l1 int primary key);
CREATE TABLE "reference'table"(r1 int primary key);
SELECT create_reference_table('reference''table');

-- replicate reference table to coordinator
SELECT master_add_node('localhost', :master_port, groupId => 0);

-- foreign key from local table to reference table --

-- these should work
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES "reference'table"(r1);
INSERT INTO "reference'table" VALUES (21);
INSERT INTO local_table VALUES (21);
-- this should fail with an appropriate error message like we do for reference tables that
-- do not need to be escaped
INSERT INTO local_table VALUES (22);

-- DROP CONSTRAINT for next commands
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;

-- these should also work
ALTER TABLE "reference'table" ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);
INSERT INTO local_table VALUES (23);
INSERT INTO "reference'table" VALUES (23);
-- this should fail with an appropriate error message like we do for reference tables that
-- do not need to be escaped
INSERT INTO local_table VALUES (24);

-- TODO: hooop

-- DROP tables finally
DROP TABLE local_table, "reference'table";

--- CREATE TABLE commands defining foreign key constraint between local tables and reference tables ---

-- remove master node from pg_dist_node for next tests to show that
-- behaviour does not need us to add coordinator to pg_dist_node priorly,
-- as it is not implemented in the ideal way (for now)
SELECT master_remove_node('localhost', :master_port);

-- create tables
CREATE TABLE reference_table (r1 int);
CREATE TABLE local_table (l1 int REFERENCES reference_table(r1));

-- actually, we did not implement upgrading "a local table referenced by another local table"
-- to a reference table yet -in an ideal way-. But it should work producing a warning
SELECT create_reference_table("reference_table");

-- show that we are checking for foreign key constraint after defining

-- this should fail
INSERT INTO local_table VALUES (31);

INSERT INTO reference_table VALUES (31);

-- this should work
INSERT INTO local_table VALUES (31);

-- that amount of test for CREATE TABLE commands defining an foreign key constraint
-- from a local table to a reference table is sufficient it is already tested
-- in some other regression tests already

-- DROP tables finally
DROP TABLE local_table;
DROP TABLE "reference'table";

-- create tables
CREATE TABLE local_table (l1 int);
CREATE TABLE reference_table (r1 int REFERENCES local_table(l1));

-- we did not implement upgrading "a local table referencing to another
-- local table" to a reference table yet.
-- this should fail
SELECT create_reference_table("reference_table");

-- finalize the test, clear the schema created for this test --
DROP SCHEMA fkey_reference_local_table;
