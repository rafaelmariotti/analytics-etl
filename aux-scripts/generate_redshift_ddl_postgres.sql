SELECT 'create external table datawarehouse.'
  || table_name
  || E'(\n'
  || string_agg('  '
  || column_name
  || ' '
  || data_type, E',\n')
  || E'\n) stored as parquet\n'
  || 'location ''s3://datalake/' || table_name || E'.parquet'';\n\n'
  AS command
FROM
  (SELECT table_name,
    column_name,
    CASE
      WHEN data_type = 'character varying' or data_type = 'text' or data_type = 'uuid' or data_type = 'json'
        THEN 'varchar'
      WHEN data_type LIKE 'timestamp without time zone'
        THEN 'timestamp'
      WHEN data_type = 'numeric' or data_type = 'double precision' /* missing some long kind data types */
        THEN 'decimal(38,10)'
      ELSE data_type
    END AS data_type
  FROM information_schema.columns
  WHERE table_name = '&table_name'
  and data_type != 'ARRAY' /* ignoring array data types from redshift - there is no data type to convert to */
  ORDER BY ordinal_position
  ) AS foo
GROUP BY table_name;
