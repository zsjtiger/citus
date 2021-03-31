/* columnar--10.1-1--10.0-3.sql */

-- TODO: populate row_count again and make it NOT NULL, or ... ?
ALTER TABLE columnar.chunk_group ADD COLUMN row_count bigint;

-- define foreign keys between columnar metadata tables
ALTER TABLE columnar.chunk
ADD FOREIGN KEY (storage_id, stripe_num, chunk_group_num)
REFERENCES columnar.chunk_group(storage_id, stripe_num, chunk_group_num) ON DELETE CASCADE;

ALTER TABLE columnar.chunk_group
ADD FOREIGN KEY (storage_id, stripe_num)
REFERENCES columnar.stripe(storage_id, stripe_num) ON DELETE CASCADE;
