# development
export AWS_ACCESS_KEY_ID="???"
export AWS_SECRET_ACCESS_KEY="???"

for config_file in $(ls | grep -v "sync_emr_config_file.sh")
do
    aws s3 cp $config_file s3://datalake/emr/$config_file &
done

#production
export AWS_ACCESS_KEY_ID="???"
export AWS_SECRET_ACCESS_KEY="???"

for config_file in $(ls | grep -v "sync_emr_config_file.sh")
do
    aws s3 cp $config_file s3://datalake/emr/$config_file &
done
