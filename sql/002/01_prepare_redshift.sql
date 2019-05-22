/*
host: analytics.redshift.com.br
port: 5432
database name: analytics
username: analytics
*/

-- change
create user analytics with password '&password';

-- change IAM for dev/prod environment
create external schema datalake from data catalog database 'datalake' iam_role ':arn_for_redshift' create external database if not exists;

grant usage on schema datalake to analytics;

alter schema datalake owner to analytics;
