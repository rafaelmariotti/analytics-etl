/*
host: analytics.redshift.com.br
port: 5439
database name: analytics
username: analytics
*/

--external tables for datalake schema
create external table datalake.dimension_one (
  id integer,
  username text,
  age integer,
  company text,
  address text
) stored as parquet 
location 's3://datalake/etl/dimension_one.parquet/';

create external table datalake.dimension_two (
  id integer,
  item_name text,
  brand text,
  item_box_amount float,
  valid_until timestamp
) stored as parquet 
location 's3://datalake/etl/dimension_two.parquet/';

create external table datalake.fact_one (
  id_dim_one integer,
  id_dim_two integer,
  created_at timestamp,
  sales_amount float,
  payment_amount float
) stored as parquet 
location 's3://datalake/etl/fact_one.parquet/';
