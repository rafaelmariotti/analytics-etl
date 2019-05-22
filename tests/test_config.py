from scripts.config.config import get_config


class TestConfig:
    def test_get_config(self):
        etl_config_path = "tests/conf/oltp-to-hdfs-s3.yml"
        conf = get_config(etl_config_path)

        expected = {'default':
                    {'global':
                        {'default_timezone': 'America/Sao_Paulo',
                         'description': 'oltp-extract-{}-{}-to-{}-file'},
                     'metadata':
                        {'hostname': 'test',
                         'username': 'spark_test',
                         'password': 'spark_test',
                         'database': 'spark_test',
                         'port': 5432}}}

        assert conf == expected
