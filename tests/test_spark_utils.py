from scripts.spark.utils import create_spark_url


class TestSparkUtils:
    def test_create_spark_url_postgres(self):
        hostname = 'postgres'
        port = 5432
        username = 'postgres'
        password = 'postgres'
        database = 'test'
        db_type = 'postgres'
        spark_url = create_spark_url(hostname, port, username, password, database, db_type)

        expected = "jdbc:postgres://postgres:5432/test?user=postgres&password=postgres&port=5432"

        assert spark_url == expected

    def test_create_spark_url_oracle(self):
        hostname = 'oracle'
        port = 1521
        username = 'oracle'
        password = 'oracle'
        database = 'test'
        db_type = 'oracle'
        spark_url = create_spark_url(hostname, port, username, password, database, db_type)

        expected = "jdbc:oracle:thin:oracle/oracle@//oracle:1521/test"

        assert spark_url == expected
