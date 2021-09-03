CREATE OR REPLACE FUNCTION pg_catalog.get_missing_time_partition_ranges(
    table_name regclass,
    to_date timestamptz,
    start_from timestamptz DEFAULT now(),
    partition_interval INTERVAL DEFAULT NULL)
returns table(
    partition_name text,
    range_from_value text,
    range_to_value text)
LANGUAGE plpgsql
AS $$
DECLARE
    /* properties of the partitioned table */
    table_name_text text;
    number_of_partition_columns int;
    partition_column_index int;
    partition_column_type regtype;

    distinct_partition_interval_count int;
    table_partition_interval INTERVAL;
    is_multiple_days boolean;
    manual_partition_from_value_text text;
    manual_partition_to_value_text text;
    current_range_from_value timestamptz := NULL;
    current_range_to_value timestamptz := NULL;
    current_range_from_value_text text;
    current_range_to_value_text text;
    datetime_string_format text;
    max_table_name_length int := current_setting('max_identifier_length');
BEGIN
    /* check whether the table is time partitioned table, if not error out */
    SELECT relname, partnatts, partattrs[0]
    INTO table_name_text, number_of_partition_columns, partition_column_index
    FROM pg_catalog.pg_partitioned_table, pg_catalog.pg_class c
    WHERE partrelid = c.oid AND c.oid = table_name;

    IF NOT FOUND THEN
        RAISE '% is not partitioned', table_name;
    ELSIF number_of_partition_columns <> 1 THEN
        RAISE 'partitioned tables with multiple partition columns are not supported';
    END IF;

    BEGIN
		PERFORM from_value::timestamptz, to_value::timestamptz
		FROM pg_catalog.time_partitions
		WHERE parent_table = table_name;
	EXCEPTION WHEN invalid_datetime_format THEN
		RAISE 'partition column of % cannot be cast to a timestamptz', table_name;
	END;

    /* get datatype here to check interval-table type alignment and generate range values in the right data format */
    SELECT atttypid
    INTO partition_column_type
    FROM pg_attribute
    WHERE attrelid = table_name::oid
    AND attnum = partition_column_index;

    IF partition_column_type = 'date'::regtype AND partition_interval IS NOT NULL THEN
        SELECT date_trunc('day', partition_interval) = partition_interval
        INTO is_multiple_days;

        IF NOT is_multiple_days THEN
            RAISE 'partition interval of date partitioned column must be multiple days';
        END IF;
    END IF;

    /*
     * Check distinct partition interval count for the given table.
     *
     * If it is 0, that means we are creating the first partition.
     * We must use the given partition_interval.
     *
     * If it is 1, that means all partitions cover the same interval.
     * We must use that interval and it must be equal to given partition_interval, if it is given.
     *
     * If it is more than 1, that means partitions cover different intervals.
     * We must error out, as partitions with different intervals are not supported
     *
     * Note that, to_value and from_value is equal to '', if default partition exist.
     * To skip them, additional checks are added to the query.
     *
     * TODO: Should we lock parent table to not to create partition in parallel?
     */
    SELECT
    COUNT(DISTINCT to_value::timestamptz - from_value::timestamptz)
    INTO distinct_partition_interval_count
    FROM time_partitions
    WHERE parent_table = table_name AND to_value <> '' AND from_value <> '';

    IF distinct_partition_interval_count = 0 THEN
        table_partition_interval := partition_interval;

        IF partition_interval IS NULL THEN
            RAISE 'must specify a partition_interval when there are no partitions yet';
        END IF;

        /* if no start time is specified, we go back by 7 partition interval */
        IF start_from IS NULL THEN
            start_from := now() - 7 * table_partition_interval;
        END IF;

        /*
         * Decide on the current_range_from_value of the initial partition according to interval of the table.
         * Since we will create all other partitions by adding intervals, truncating given start time will provide
         * more intuitive interval ranges, instead of starting from start_from directly.
         * TODO: Check truncate for quarter
         */
        IF table_partition_interval < INTERVAL '1 hour' THEN
            current_range_from_value = date_trunc('minute', start_from);
        ELSIF table_partition_interval < INTERVAL '1 day' THEN
            current_range_from_value = date_trunc('hour', start_from);
        ELSIF table_partition_interval < INTERVAL '1 week' THEN
            current_range_from_value = date_trunc('day', start_from);
        ELSIF table_partition_interval < INTERVAL '1 month' THEN
            current_range_from_value = date_trunc('week', start_from);
        ELSIF table_partition_interval = INTERVAL '3 months' THEN
            current_range_from_value = date_trunc('quarter', start_from);
        ELSIF table_partition_interval < INTERVAL '1 year' THEN
            current_range_from_value = date_trunc('month', start_from);
        ELSE
            current_range_from_value = date_trunc('year', start_from);
        END IF;

        current_range_to_value := current_range_from_value + table_partition_interval;

    ELSIF distinct_partition_interval_count = 1 THEN
        SELECT
        DISTINCT to_value::timestamptz - from_value::timestamptz
        INTO table_partition_interval
        FROM time_partitions
        WHERE parent_table = table_name AND to_value <> '' AND from_value <> '';

        IF partition_interval IS NOT NULL AND partition_interval <> table_partition_interval THEN
            RAISE 'partition_interval does not match existing partitions'' interval';
        END IF;

        /* use initial partition as pivot to find range for missing partitions */
        SELECT from_value::timestamptz, to_value::timestamptz
        INTO current_range_from_value, current_range_to_value
        FROM pg_catalog.time_partitions
        WHERE parent_table = table_name
        ORDER BY from_value::timestamptz ASC
        LIMIT 1;

        /* if start_from is newer than pivot's from value, go forward, else go backward */
        IF start_from >= current_range_from_value THEN
            WHILE current_range_from_value < start_from LOOP
                    current_range_from_value := current_range_from_value + table_partition_interval;
            END LOOP;
            current_range_to_value := current_range_from_value + table_partition_interval;
        ELSE
            WHILE current_range_from_value > start_from LOOP
                    current_range_from_value := current_range_from_value - table_partition_interval;
            END LOOP;
            current_range_to_value := current_range_from_value + table_partition_interval;
        END IF;

    ELSIF distinct_partition_interval_count > 1 THEN
        RAISE 'each partition must cover same interval to use that function'; --TODO: Check the message
    END IF;

    /* reuse pg_partman naming scheme for back-and-forth migration */
    IF table_partition_interval = INTERVAL '3 months' THEN
        /* include quarter in partition name */
        datetime_string_format = 'YYYY"q"Q';
    ELSIF table_partition_interval = INTERVAL '1 week' THEN
        /* include week number in partition name */
        datetime_string_format := 'IYYY"w"IW';
    ELSE
        /* in all other cases, start with the year */
        datetime_string_format := 'YYYY';

        IF table_partition_interval < INTERVAL '1 year' THEN
            /* include month in partition name */
            datetime_string_format := datetime_string_format || '_MM';
        END IF;

        IF table_partition_interval < INTERVAL '1 month' THEN
            /* include day of month in partition name */
            datetime_string_format := datetime_string_format || '_DD';
        END IF;

        IF table_partition_interval < INTERVAL '1 day' THEN
            /* include time of day in partition name */
            datetime_string_format := datetime_string_format || '_HH24MI';
        END IF;

        IF table_partition_interval < INTERVAL '1 minute' THEN
             /* include seconds in time of day in partition name */
             datetime_string_format := datetime_string_format || 'SS';
        END IF;
    END IF;

    WHILE current_range_from_value < to_date LOOP
        /*
         * Check whether partition with given range has already been created
         * Since partition interval can be given with different types, we are converting
         * all variables to timestamptz to make sure that we are comparing same type of parameters
         */
        PERFORM * FROM pg_catalog.time_partitions
        WHERE
            from_value::timestamptz = current_range_from_value::timestamptz AND
            to_value::timestamptz = current_range_to_value::timestamptz AND
            parent_table = table_name;
        IF found THEN
            current_range_from_value := current_range_to_value;
            current_range_to_value := current_range_to_value + table_partition_interval;
            CONTINUE;
        END IF;

        /*
         * Check whether any other partition covers from_value or to_value
         * That means some partitions doesn't align with the initial partition.
         * In other words, gap(s) exist between partitions which is not multiple of intervals.
         */
        SELECT from_value::text, to_value::text
        INTO manual_partition_from_value_text, manual_partition_to_value_text
        FROM pg_catalog.time_partitions
        WHERE
            ((current_range_from_value::timestamptz > from_value::timestamptz AND current_range_from_value < to_value::timestamptz) OR
            (current_range_to_value::timestamptz > from_value::timestamptz AND current_range_to_value::timestamptz < to_value::timestamptz)) AND
            parent_table = table_name;

        IF found THEN
            RAISE 'Partition with the range from % to % does not align with the initial partition. Please make sure that no gap(s) exists between existing partitions', -- TODO: Check the message
            manual_partition_from_value_text,
            manual_partition_to_value_text;
        END IF;

        IF partition_column_type = 'date'::regtype THEN
            SELECT current_range_from_value::date::text INTO current_range_from_value_text;
            SELECT current_range_to_value::date::text INTO current_range_to_value_text;
        ELSIF partition_column_type = 'timestamp without time zone'::regtype THEN
            SELECT current_range_from_value::timestamp::text INTO current_range_from_value_text;
            SELECT current_range_to_value::timestamp::text INTO current_range_to_value_text;
        ELSIF partition_column_type = 'timestamp with time zone'::regtype THEN
            SELECT current_range_from_value::timestamptz::text INTO current_range_from_value_text;
            SELECT current_range_to_value::timestamptz::text INTO current_range_to_value_text;
        ELSE
            RAISE 'type of the partition column of the table % must be date, timestamp or timestamptz', table_name;
        END IF;

        /* use range values within the name of partition to have unique partition names */
        RETURN QUERY
        SELECT
            substring(table_name_text, 0, max_table_name_length - length(to_char(current_range_from_value, datetime_string_format)) - 1) || '_p' ||
            to_char(current_range_from_value, datetime_string_format),
            current_range_from_value_text,
            current_range_to_value_text;

        current_range_from_value := current_range_to_value;
        current_range_to_value := current_range_to_value + table_partition_interval;
    END LOOP;

    RETURN;
END;
$$;

COMMENT ON FUNCTION pg_catalog.get_missing_time_partition_ranges(
	table_name regclass,
    to_date timestamptz,
    start_from timestamptz,
    partition_interval INTERVAL)
IS 'get missing partitions ranges for table within the range using the given interval';
