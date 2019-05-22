/*
host: localhost
port: 5432
database name: spark_conf
username: spark_conf
*/

create schema test;

create table test.stage_date_column(
  id serial,
  date_column varchar(30),
  constraint pk_stage_date_column primary key(id)
);

create table test.stage_table(
  table_name varchar(30),
  partition_column varchar(30),
  num_partitions integer default 8,
  date_column_id integer,
  constraint pk_stage_table primary key(table_name),
  constraint fk_date_column foreign key (date_column_id) references test.stage_date_column(id)
);

insert into test.stage_date_column (date_column) values ('updated_at');

insert into test.stage_table (table_name, partition_column, date_column_id) values ('test', 'split_col', 1);

update test.stage_table set num_partitions = 2048 where table_name = 'test';
