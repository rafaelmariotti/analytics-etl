#!/bin/bash

#############################################################################
# script: Generate best partition number for spark                          #
# developed by: Rafael Mariotti                                             #
#                                                                           #
# arguments: db_hostname db_service_name db_schema_user db_schema_password  #
#############################################################################

function get_best_cost_partition_number {
    metadata_host="???"
    metadata_user="???"
    metadata_password="???"
    metadata_database="???"


    # connect to metadata to get tables and split column names
    export PGPASSWORD="${metadata_password}"
    psql -h ${metadata_host} -U ${metadata_user} -d ${metadata_database} > /dev/null <<EOF
    \t
    \o /tmp/metadata_info.log;
    select table_name || ',' || partition_column as table_info from ${db_schema_user}.stage_table;
    \q
EOF

    for table_info in $(cat /tmp/metadata_info.log | grep -v -e "^$")
    do
        table_name="$(echo ${table_info} | awk -F, '{print $1}')"
        column_name="$(echo ${table_info} | awk -F, '{print $2}')"

        # connect to oracle and calculate best partition number based on their explain costs
        connect_url="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=${db_hostname})(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=${db_service_name})))"
        sqlplus -S ${db_schema_user}/${db_schema_password}@"${connect_url}" > /dev/null <<EOF
            set serveroutput on;
            set heading off;
            set echo off;
            set feedback off;
            set verify off;
            set newpage none;
            set wrap off;
            set linesize 32767;
            set trimspool on;

            spool '/tmp/partition_result.log';

            DECLARE
              TABLE_NAME VARCHAR2(30) := '${table_name}';
              COLUMN_NAME VARCHAR2(30) := '${column_name}';
              PARTITION_NUMBER NUMBER := 1;
              COST_RESULT      NUMBER := 0;
              COST_THRESHOLD   NUMBER := 5000;
              MIN_ID           NUMBER;
              MAX_ID           NUMBER;
            BEGIN
              --get all tables
              PARTITION_NUMBER := 2;
              COST_RESULT      := 0;

              --get min/max value for split column
              EXECUTE IMMEDIATE 'select min(' || COLUMN_NAME || ') from ' || TABLE_NAME INTO min_id;
              EXECUTE IMMEDIATE 'select max(' || COLUMN_NAME || ') from ' || TABLE_NAME INTO max_id;
              WHILE( TRUE )
              LOOP
                --create explain plan
                EXECUTE immediate 'EXPLAIN PLAN FOR  select * from ' || TABLE_NAME || ' where ' || COLUMN_NAME || ' between ' || MIN_ID || ' and (' || MAX_ID || ' - ' || MIN_ID || ')/' || PARTITION_NUMBER || ' + ' || MIN_ID;
                --get cost
                SELECT COST
                INTO COST_RESULT
                FROM plan_table
                WHERE plan_id =
                  ( SELECT MAX(plan_id) FROM plan_table
                  )
                AND id = 0;

                --check if its the best cost result
                IF(COST_RESULT <= COST_THRESHOLD) THEN
                  EXIT;
                ELSE
                  --duplicate partition number to check next cost result
                  SELECT PARTITION_NUMBER*2 INTO PARTITION_NUMBER FROM DUAL;
                END IF;
              END LOOP;

              --print result
              DBMS_OUTPUT.PUT_LINE(PARTITION_NUMBER);
            END;
            /
            spool off;
EOF
        num_partitions="$(cat /tmp/partition_result.log)"
        echo "update ${db_schema_user}.stage_table set num_partitions=${num_partitions} where table_name='${table_name}';"
        rm -f /tmp/partition_result.log /tmp/metadata_info.log
    done
}

function main {
    db_hostname=$1
    db_service_name=$2
    db_schema_user=$3
    db_schema_password=$4
    percent=$4
    degree=$5

    get_best_cost_partition_number
}

main $@
