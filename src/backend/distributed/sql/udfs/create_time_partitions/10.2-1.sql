CREATE OR REPLACE FUNCTION pg_catalog.create_time_partitions(
    table_name regclass,
    to_date timestamptz,
    start_from timestamptz DEFAULT NULL,
    partition_interval INTERVAL DEFAULT NULL)
returns boolean
LANGUAGE plpgsql
AS $$
DECLARE
    missing_partition_record record;
    schema_name_text text;
    table_name_text text;
BEGIN
    SELECT nspname, relname
    INTO schema_name_text, table_name_text
    FROM pg_class JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
    WHERE pg_class.oid = table_name::oid;

    /*
     * Get missing partition range info using the get_missing_partition_ranges
     * and create partitions using that info.
     */
    FOR missing_partition_record IN
        SELECT *
        FROM get_missing_time_partition_ranges(table_name, to_date, start_from, partition_interval)
    LOOP
        EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (''%s'') TO (''%s'')',
        schema_name_text,
        missing_partition_record.partition_name,
        schema_name_text,
        table_name_text,
        missing_partition_record.range_from_value,
        missing_partition_record.range_to_value);
    END LOOP;

    RETURN true;
END;
$$;
COMMENT ON FUNCTION pg_catalog.create_time_partitions(
    table_name regclass,
    to_date timestamptz,
    start_from timestamptz,
    partition_interval INTERVAL)
IS 'create time partitions for the given range';
