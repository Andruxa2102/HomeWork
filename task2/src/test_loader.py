from src.db import init_pipeline_run, get_last_success_version, close_pipeline_run


def test_pipeline_audit_lifecycle(db_connection):
    """Тестируем полный жизненный цикл логирования во временной БД внутри Docker"""
    identifier = "1.2.643.5.1.13.13.11.1461"
    test_version = "2026.06.22"

    # Проверяем пустую базу
    last_version = get_last_success_version(db_connection, identifier)
    actual_last_version = last_version[0] if isinstance(last_version, tuple) else last_version
    assert actual_last_version is None, "Изначально база должна быть пустой"

    # Инициализируем запуск
    run_id_row = init_pipeline_run(db_connection, identifier)
    assert run_id_row is not None, "Должен вернуться сгенерированный run_id"

    actual_run_id = run_id_row[0] if isinstance(run_id_row, tuple) else run_id_row

    # Проверяем статус 'started'
    with db_connection.cursor() as cur:
        cur.execute("SELECT pipeline_status::text FROM nsi.log_pipeline_runs WHERE run_id = %s;", (actual_run_id,))
        status = cur.fetchone()[0]
        assert status == "started", "Статус должен быть 'started'"

    # Закрываем запуск
    close_pipeline_run(
        conn=db_connection,
        run_id=actual_run_id,
        status="success",
        version=test_version,
        records=100
    )

    # Проверяем успешный статус
    with db_connection.cursor() as cur:
        cur.execute("SELECT pipeline_status::text, records_affected FROM nsi.log_pipeline_runs WHERE run_id = %s;",
                    (actual_run_id,))
        status, records = cur.fetchone()
        assert status == "success", "Статус должен стать 'success'"
        assert records == 100

    # Проверяем работу идемпотентности
    success_version_row = get_last_success_version(db_connection, identifier)
    assert success_version_row is not None
    actual_version = success_version_row[0] if isinstance(success_version_row, tuple) else success_version_row
    assert actual_version == test_version

