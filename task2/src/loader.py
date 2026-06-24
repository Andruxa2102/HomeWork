from json import JSONDecodeError, loads, dumps
from os import getenv
from math import ceil
from time import sleep, time
from logging import basicConfig, INFO, getLogger
from hashlib import sha256
from psycopg2 import Error
from httpx import Client, HTTPError
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from src.db import get_db_connection, init_pipeline_run, get_last_success_version, close_pipeline_run


load_dotenv()

basicConfig(level=INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = getLogger("nsi_loader.main")

BASE_URL = "https://nsi.rosminzdrav.ru"
URL_VERSION = f"{BASE_URL}/port/rest/versions"
URL_DATA = f"{BASE_URL}/port/rest/data"

IDENTIFIER = "1.2.643.5.1.13.13.11.1461"
CHUNK_SIZE = 1000
USER_KEY = getenv("NSI_USER_KEY")



def fetch_version_from_api() -> str:
    """Запрашивает у API Минздрава актуальную версию справочника"""
    page, size = 1, 1
    params = {"identifier": IDENTIFIER, "userKey": USER_KEY, "page": page, "size": size}


    try:
        with Client(verify=False, timeout=30.0) as client:
            response = client.get(URL_VERSION, params=params)
        response.raise_for_status()
    except HTTPError as e:
        raise RuntimeError(f"API Минздрава недоступно: {e}")

    clean_text = response.text.replace("'", '"') \
        .replace("None", "null") \
        .replace("True", "true") \
        .replace("False", "false")

    try:
        data = loads(clean_text)
    except JSONDecodeError as e:
        raise RuntimeError(f"API Минздрава вернуло некорректный ответ: {e}")

    data_version = data.get("list", [])[0].get("version")

    if data_version:
        return data_version
    else:
        raise RuntimeError("API Минздрава вернуло ответ, но спарсить версию справочника не удалось")


def pipeline_stream_loader(client):
    """Постранично скачивает данные с ретраями"""
    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        logger.info(f"Запрос страницы {current_page} из {total_pages if total_pages > 1 else '...'}...")
        params = {"identifier": IDENTIFIER, "userKey": USER_KEY, "size": CHUNK_SIZE, "page": current_page}

        max_retries = 3
        attempt = 0
        success = False

        while attempt < max_retries:
            try:
                response = client.get(URL_DATA, params=params)
                if response.status_code != 200:
                    attempt += 1
                    logger.warning(
                        f"Ошибка API (Код {response.status_code}). Попытка {attempt}/{max_retries}. Ожидание 5 сек...")
                    sleep(5)
                    continue

                clean_text = (
                    response.text.replace("'", '"')
                    .replace("None", "null")
                    .replace("True", "true")
                    .replace("False", "false")
                )
                payload = loads(clean_text)
                success = True
                break

            except (HTTPError, JSONDecodeError):
                attempt += 1
                logger.warning(
                    f"Сбой сети/парсинга на странице {current_page}. Попытка {attempt}/{max_retries}. Ожидание 5 сек...")
                sleep(5)

        if not success:
            raise RuntimeError(
                f"Критическая ошибка сети: не удалось загрузить страницу {current_page} после {max_retries} попыток")

        if current_page == 1:
            total_records = payload.get("total", 0)
            total_pages = ceil(total_records / CHUNK_SIZE)
            logger.info(f"В реестре обнаружено {total_records} записей. Всего страниц: {total_pages}")
            if total_records == 0:
                break

        raw_list = payload.get("list", [])
        if not raw_list:
            break

        yield raw_list
        current_page += 1
        sleep(0.5)


def main():
    if not USER_KEY:
        logger.error("Критическая ошибка: Переменная USER_KEY не найдена в .env!")
        return

    run_id = None
    total_inserted = 0

    try:
        db_conn = get_db_connection()
    except Error as conn_err:
        logger.critical(f"Не удалось установить начальное соединение с БД: {conn_err}")
        return

    try:
        try:
            api_version = fetch_version_from_api()
        except (HTTPError, JSONDecodeError) as version_err:
            raise RuntimeError(f"Не удалось получить или распарсить версию справочника от API: {version_err}")

        last_success = get_last_success_version(db_conn, IDENTIFIER)
        if last_success and last_success == api_version:
            logger.info(f"Версия {api_version} уже загружена ранее. Запуск отменен")
            return

        run_id = init_pipeline_run(db_conn, IDENTIFIER)
        logger.info(f"Запуск зарегистрирован в БД. Присвоен run_id: {run_id}")

        with Client(verify=False, timeout=120.0) as client:
            for raw_chunk in pipeline_stream_loader(client):
                start_time = time()
                db_records = []

                for org_raw_data in raw_chunk:
                    # Находим бизнес-идентификатор 'id' организации внутри структуры
                    org_id = next((item.get("value") for item in org_raw_data if item.get("column") == "id"), None)
                    if not org_id:
                        logger.warning("Обнаружена запись без идентификатора организации, пропускаем")
                        continue

                    # Хеш от конкатенации идентификатора организации + source_version
                    concat_string = f"{org_id}{api_version}"
                    raw_org_hash_key = sha256(concat_string.encode('utf-8')).hexdigest()

                    payload_json_string = dumps(org_raw_data, ensure_ascii=False)
                    db_records.append((
                        str(raw_org_hash_key),
                        str(api_version),
                        payload_json_string
                    ))

                if not db_records:
                    continue

                try:
                    with db_conn.cursor() as cur:
                        execute_values(
                            cur,
                            """
                            INSERT INTO nsi.raw_medical_organizations (raw_org_hash_key, source_version, payload)
                            VALUES %s
                            ON CONFLICT (raw_org_hash_key) DO UPDATE SET 
                                payload = EXCLUDED.payload,
                                loaded_at = NOW()
                            """,
                            db_records,
                            template="(%s, %s, %s::jsonb)"
                        )
                    db_conn.commit()
                except Error as db_page_err:
                    db_conn.rollback()
                    logger.error(f"Ошибка транзакции при вставке пакета в БД: {db_page_err}")
                    raise

                elapsed_time = time() - start_time
                total_inserted += len(db_records)
                speed = len(db_records) / elapsed_time if elapsed_time > 0 else len(db_records)
                logger.info(f"Пакет успешно сохранен ({speed:.1f} стр/сек). Всего в базе: {total_inserted}")

        close_pipeline_run(db_conn, run_id, 'success', api_version, total_inserted)
        logger.info(f"Пайплайн успешно завершен. Всего загружено объектов: {total_inserted}")

    except Exception as global_err:
        logger.critical(f"Критический сбой пайплайна: {global_err}", exc_info=True)
        if db_conn and run_id:
            try:
                close_pipeline_run(db_conn, run_id, 'failed', 'UNKNOWN', 0, error_msg=str(global_err))
            except Exception as audit_err:
                logger.error(f"Не удалось записать статус ошибки в таблицу логов: {audit_err}")

    finally:
        if db_conn:
            db_conn.close()
            logger.info("Соединение со БД закрыто")


if __name__ == "__main__":
    load_dotenv()
    main()

