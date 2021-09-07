CREATE OR REPLACE PROCEDURE pg_catalog.compress_old_partitions(
  table_name regclass,
  older_than timestamptz)
LANGUAGE plpgsql
AS $$
BEGIN
    CALL alter_old_partitions_set_access_method(table_name, older_than, 'columnar');
 END;
$$;
COMMENT ON PROCEDURE pg_catalog.compress_old_partitions(
  table_name regclass,
  older_than timestamptz)
IS 'compress old partitions of a time-partitioned table';
