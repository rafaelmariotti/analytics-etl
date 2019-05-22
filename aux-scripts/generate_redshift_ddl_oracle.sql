SELECT lower('create external table '
  || USER
  || '.'
  || table_name
  || ' ('
  || CHR(10)
  || LISTAGG('  '
  || column_name
  || ' '
  || redshift_column, ','
  || CHR(10) ) WITHIN GROUP(
ORDER BY column_id )
  || CHR(10)
  || ') stored as parquet '
  || CHR(10)
  || 'location ''s3://datalake/' || user || '/'
  || table_name
  || '.parquet'';') AS ddl_command
FROM
  (SELECT table_name,
    column_name,
    data_type,
    column_id,
    CASE
      WHEN data_type = 'DATE'
      OR data_type LIKE 'TIMESTAMP%'
      THEN 'TIMESTAMP'
      WHEN data_type = 'VARCHAR2'
      OR data_type   = 'RAW'
      THEN 'VARCHAR('
        || data_length
        || ')'
      WHEN data_type = 'NUMBER'
      THEN
        CASE
          WHEN data_precision IS NULL
          AND data_scale      IS NULL
          THEN 'DECIMAL(38,10)'
          WHEN data_precision IS NULL
          AND data_scale       = 0
          THEN 'DECIMAL(38)'
          WHEN data_precision = 38
          AND data_scale      = 0
          THEN 'DECIMAL('
            || data_precision
            || ')'
          ELSE 'FLOAT'
        END
      ELSE data_type
        || '('
        || data_length
        || ')'
    END AS redshift_column
  FROM user_tab_columns tab_col
  WHERE lower(table_name) IN (lower('&table_name'))
  ) tab_col
GROUP BY table_name
ORDER BY table_name;
