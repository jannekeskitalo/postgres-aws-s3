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
        host = compose.get_service_host("postgres", 5432)
        port = compose.get_service_port("postgres", 5432)
        print(f"Postgres running at: {host}:{port}")
        time.sleep(10)
        yield {"database": "test", "user": "test", "password": "test", "host": host, "port": port}


def install_extension(connection):
    cur = connection.cursor(cursor_factory=NamedTupleCursor)
    cur.execute("create extension plpython3u")
    cur.execute("create extension aws_s3")


def test_installation(connection_parameters):
    with psycopg2.connect(**connection_parameters) as connection:
        install_extension(connection)
        cur = connection.cursor(cursor_factory=NamedTupleCursor)
        cur.execute("select * from pg_extension where extname = 'aws_s3'")
        row = cur.fetchone()
    assert row.extname == "aws_s3" 
