import pytest
from testcontainers.compose import DockerCompose
import psycopg2
from psycopg2.extras import NamedTupleCursor
import time


@pytest.fixture(scope="session")
def connection_parameters():
    with DockerCompose(
        ".", compose_file_name=["docker-compose.yml"], build=True
    ) as compose:
        db_host = compose.get_service_host("postgres", 5432)
        db_port = compose.get_service_port("postgres", 5432)
        s3_port = compose.get_service_port("localstack", 4566)
        print(f"Postgres running at: {db_host}:{db_port}")
        time.sleep(10)
        yield {
            "postgres": {
                "database": "test",
                "user": "test",
                "password": "test",
                "host": db_host,
                "port": db_port,
            },
            "s3_endpoint_url": f"http://localstack:{s3_port}",
        }


def install_extension(connection):
    cur = connection.cursor(cursor_factory=NamedTupleCursor)
    cur.execute("create extension plpython3u")
    cur.execute("create extension aws_s3")


def test_installation(connection_parameters):
    with psycopg2.connect(**connection_parameters["postgres"]) as connection:
        install_extension(connection)
        cur = connection.cursor(cursor_factory=NamedTupleCursor)
        cur.execute("select * from pg_extension where extname = 'aws_s3'")
        row = cur.fetchone()
    assert row.extname == "aws_s3"


def test_s3_export(connection_parameters):
    with psycopg2.connect(**connection_parameters["postgres"]) as connection:
        cur = connection.cursor(cursor_factory=NamedTupleCursor)
        cur.execute("create table public.foo as select md5(random()::text) as bar from generate_series(1,100)")
        cur.execute("select count(*) as cnt from foo")
        row = cur.fetchone()
        assert row.cnt == 100
        export_sql = f"""
        select * from
            aws_s3.query_export_to_s3(
                'select * from public.foo',
                'test-bucket',
                'foo.csv',
                'eu-west-1',
                'dummy',
                'dummy',
                'dummy',
                options := 'FORMAT CSV, HEADER true',
                endpoint_url := '{connection_parameters["s3_endpoint_url"]}'
            )
        """
        cur.execute(export_sql)
    input("Wait...")