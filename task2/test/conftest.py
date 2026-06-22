from os import getenv, path
from pytest import fixture
from psycopg2 import connect
from pytest_postgresql.janitor import DatabaseJanitor
from dotenv import load_dotenv

load_dotenv()


@fixture(scope="function")
def db_connection():
    """Фикстура для создания тестовой среды"""
    pg_params = {
        "host": getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(getenv("POSTGRES_PORT", "5432")),
        "user": getenv("POSTGRES_USER", "nsi_admin"),
        "password": getenv("POSTGRES_PASSWORD", "super_secret_password_99"),
        "dbname": "pytest_nsi_test"
    }

    with DatabaseJanitor(
            user=pg_params["user"],
            host=pg_params["host"],
            port=pg_params["port"],
            dbname=pg_params["dbname"],
            version=14,
            password=pg_params["password"]
    ):
        conn = connect(**pg_params)

        root_dir = path.dirname(path.dirname(path.abspath(__file__)))
        init_sql_path = path.join(root_dir, "sql", "init.sql")

        with open(init_sql_path, "r", encoding="utf-8") as f:
            init_sql = f.read()

        with conn.cursor() as cur:
            cur.execute(init_sql)
        conn.commit()

        yield conn
        conn.close()

