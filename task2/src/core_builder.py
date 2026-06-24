from os import path
from logging import basicConfig, INFO, getLogger
from dotenv import load_dotenv
from src.db import get_db_connection

load_dotenv()

basicConfig(level=INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = getLogger("nsi_core.builder")

CURRENT_DIR = path.dirname(path.abspath(__file__))
PROJECT_ROOT = path.dirname(CURRENT_DIR)
SQL_DIR = path.join(PROJECT_ROOT, 'sql', 'core')


def run_sql_file(cursor, file_name):
    """Читает и выполняет SQL-файл"""
    file_path = path.join(SQL_DIR, file_name)
    logger.info(f"Выполнение скрипта: {file_name}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_query = f.read()
    cursor.execute(sql_query)

def main():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            run_sql_file(cur, "load_hub_organization.sql")
            run_sql_file(cur, "load_sat_organization_attrs.sql")
            run_sql_file(cur, "load_sat_organization_changes.sql")
        conn.commit()
        logger.info("Заполнение таблиц успешно завершено")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при расчете Core-слоя: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

