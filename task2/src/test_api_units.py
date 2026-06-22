from pytest import raises
from httpx import TimeoutException, Client
from src.loader import fetch_version_from_api, pipeline_stream_loader, IDENTIFIER, USER_KEY, URL_DATA, URL_VERSION


def test_api_available(httpx_mock):
    """Проверяем, что при успешном ответе 200 функция корректно извлекает версию"""
    expected_version = "6.2035"

    httpx_mock.add_response(
        url=f"{URL_VERSION}?identifier={IDENTIFIER}&userKey={USER_KEY}&page=1&size=1",
        status_code=200,
        text="{'list': [{'version': '6.2035'}]}"
    )

    version = fetch_version_from_api()

    assert version == expected_version
    assert isinstance(version, str)


def test_api_timeout(httpx_mock):
    """Проверяем, что таймаут сети перехватывается и оборачивается в RuntimeError"""

    httpx_mock.add_exception(
        TimeoutException("Запрос превысил время ожидания"),
        url=f"{URL_VERSION}?identifier={IDENTIFIER}&userKey={USER_KEY}&page=1&size=1"
    )

    with raises(RuntimeError) as exc_info:
        fetch_version_from_api()

    assert "API Минздрава недоступно" in str(exc_info.value)


def test_api_500_retry(httpx_mock, monkeypatch):
    """Проверяем, что пагинатор ретраит при ошибке 500 и падает только после исчерпания попыток"""
    monkeypatch.setattr("time.sleep", lambda x: None)

    url_data = f"{URL_DATA}?identifier={IDENTIFIER}&userKey={USER_KEY}&size=1000&page=1"

    for _ in range(3):
        httpx_mock.add_response(url=url_data, status_code=500)

    with Client(verify=False) as client:
        generator = pipeline_stream_loader(client)

        with raises(RuntimeError) as exc_info:
            next(generator)

        assert "не удалось загрузить страницу 1 после 3 попыток" in str(exc_info.value)

