from logging import INFO
from re import compile
from src.loader import main, IDENTIFIER
from src.db import init_pipeline_run, close_pipeline_run


class NonClosableConnection:
    """Обертка над соединением, которая игнорирует вызовы close()"""

    def __init__(self, real_conn):
        self._real_conn = real_conn

    def __getattr__(self, name):
        """Любой метод или атрибут, кроме close() передаем реальному psycopg2-connection"""
        return getattr(self._real_conn, name)

    def close(self):
        """Перехватываем close() и ничего не делаем, чтобы тестовая БД не закрылась"""
        pass


def test_same_version_skipped(db_connection, httpx_mock, caplog, monkeypatch):
    """Проверяем, если версия совпадает с прошлой успешной, загрузка отменяется"""
    test_version = "6.2035"

    monkeypatch.setattr("src.loader.USER_KEY", "test_dummy_key_for_pytest")

    run_id_row = init_pipeline_run(db_connection, IDENTIFIER)
    actual_run_id = run_id_row[0] if isinstance(run_id_row, tuple) else run_id_row

    close_pipeline_run(
        conn=db_connection,
        run_id=actual_run_id,
        status="success",
        version=test_version,
        records=500
    )

    url_pattern = compile(r".*/versions($|\?)")
    httpx_mock.add_response(
        url=url_pattern,
        status_code=200,
        text=f"{{'list': [{{'version': '{test_version}'}}]}}"
    )

    with caplog.at_level(INFO):
        import src.loader
        src.loader.get_db_connection = lambda: NonClosableConnection(db_connection)

        main()

    assert any("Версия" in record.message for record in caplog.records)
    assert any("Запуск отменен" in record.message for record in caplog.records)


def test_new_version_triggers_load(db_connection, httpx_mock, caplog, monkeypatch):
    """Проверяем, что если на источнике вышла новая версия, пайплайн начинает загрузку"""
    old_version = "5.1100"
    new_version = "6.2035"

    run_id_row = init_pipeline_run(db_connection, IDENTIFIER)
    actual_run_id = run_id_row[0] if isinstance(run_id_row, tuple) else run_id_row
    close_pipeline_run(db_connection, actual_run_id, "success", old_version, 100)

    url_pattern = compile(r".*/versions($|\?)")
    httpx_mock.add_response(url=url_pattern, status_code=200, text=f"{{'list': [{{'version': '{new_version}'}}]}}")

    data_pattern = compile(r".*/data($|\?)")
    httpx_mock.add_response(url=data_pattern, status_code=200, text="{'total': 0, 'list': []}")

    with caplog.at_level(INFO):
        import src.loader
        src.loader.get_db_connection = lambda: NonClosableConnection(db_connection)
        main()

    assert not any("уже загружена ранее" in record.message for record in caplog.records)
    assert any("Запуск зарегистрирован в БД" in record.message for record in caplog.records)

    with db_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM nsi.log_pipeline_runs;")
        runs_count = cur.fetchone()[0]
        assert runs_count == 2, "В таблице аудита должен появиться второй ран для новой версии"

