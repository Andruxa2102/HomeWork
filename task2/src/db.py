from os import getenv
from psycopg2 import connect
from logging import getLogger

logger = getLogger("nsi_loader.db")

def get_db_connection():
    """Создает подключение к Postgres"""
    return connect(
        host=getenv("POSTGRES_HOST"),
        port=getenv("POSTGRES_PORT"),
        database=getenv("POSTGRES_DB"),
        user=getenv("POSTGRES_USER"),
        password=getenv("POSTGRES_PASSWORD")
    )

def init_pipeline_run(conn, identifier: str) -> int:
    """Регистрирует запуск пайплайна"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO nsi.log_pipeline_runs (pipeline_name, source_identifier, pipeline_status)
            VALUES ('load_medical_organizations', %s, 'started'::pipeline_statuses)
            RETURNING run_id;
        """, (identifier,))
        row = cur.fetchone()
    return row[0] if row else None

def get_last_success_version(conn, identifier: str) -> str:
    """Ищет номер последней успешной версии"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_version FROM nsi.log_pipeline_runs
            WHERE source_identifier = %s AND pipeline_status = 'success'::pipeline_statuses
            ORDER BY finished_at DESC LIMIT 1;
        """, (identifier,))
        res = cur.fetchone()
        return res[0] if res else None

def close_pipeline_run(conn, run_id: int, status: str, version: str, records: int, error_msg: str = None):
    """Обновляет финальный статус пайплайна"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE nsi.log_pipeline_runs 
            SET pipeline_status = %s::pipeline_statuses, 
                source_version = %s, 
                records_affected = %s, 
                error_message = %s,
                finished_at = CURRENT_TIMESTAMP
            WHERE run_id = %s;
        """, (status.lower(), version, records, error_msg, run_id))
    conn.commit()

