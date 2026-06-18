from __future__ import annotations

import os, io, re, json, zipfile, tempfile, datetime as dt
from typing import List, Dict

import pytz, requests, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from webdav3.client import Client
from webdav3.exceptions import WebDavException

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

# ===== Параметры =====
DAG_ID = "glazkov_i38_errors_ingest_export"
TZ = "Europe/Moscow"
# SCHEDULE = "0 8 * * *"  # ежедневно 08:00
SCHEDULE = None

WEBDAV_REMOTE_DIR = "/i38_errors/"
S3_BUCKET = Variable.get("I38_BUCKET", default_var="i38-errors")
LANDING_PREFIX = "landing"
EXPORT_PREFIX  = "exports"
EXPORT_DAYS = int(Variable.get("I38_EXPORT_DAYS", 7))
SSH_CMD_TIMEOUT = int(Variable.get("I38_SSH_CMD_TIMEOUT", 1400))


ICE_MAIN_TABLE = "ice.db38.errors"
ICE_LOG_TABLE  = "ice.db38.errors_load_log"

CSV_SEP = ";"
CSV_ENCODING = "utf-8"

# Spark в контейнере jupyter на 192.168.150.29,
# скрипты проброшены в контейнер как /work/scripts/*.py
SPARK_SUBMIT_CMD = (
    "docker exec jupyter /opt/bitnami/spark/bin/spark-submit"
)
SCRIPT_INGEST_PATH = "/work/scripts/i38_ingest.py"
SCRIPT_EXPORT_PATH = "/work/scripts/i38_export_weekly.py"

SUBJECT_CHAT_MAP_VAR = "I38_SUBJECT_CHAT_MAP"
DEFAULT_SUBJECT_CHAT_MAP = {
    'г. Севастополь': '',
    'г. Санкт-Петербург': ''
}
# ===== Клиенты =====
def get_webdav_client():
    connection = BaseHook.get_connection('nextcloud_webdav')
    options = {
        'webdav_hostname': f"{connection.schema}://{connection.host}",
        'webdav_login': connection.login,
        'webdav_password': connection.password,
        'webdav_root': connection.extra_dejson.get('webdav_path'),
        'webdav_port': connection.port,
        'timeout': 60,
        'verify': False
    }
    return Client(options)

def get_s3():
    conn = BaseHook.get_connection("aws_default")
    extra = conn.extra_dejson or {}
    endpoint_url = extra.get("host") or extra.get("endpoint_url")
    region_name = extra.get("region_name", "us-east-1")
    session = boto3.session.Session()
    return session.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(signature_version="s3v4"),
    )

def make_slug(name: str) -> str:
    s = (name or "").lower().strip().replace("ё", "е")
    s = re.sub(r'^(г|г\.|гор\.|город)\s+', '', s)
    s = re.sub(r'^(республика|респ\.)\s+', '', s)
    s = s.replace(' - ', ' ').replace('-', ' ')
    s = re.sub(r'[\(\)]', ' ', s)
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[^a-zа-я0-9_]', '', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s

def get_telegram_token():
    conn = BaseHook.get_connection("i38_tg_bot_sender")
    return conn.password or (conn.extra_dejson or {}).get("token")

def is_friday(execution_dt: dt.datetime) -> bool:
    tz = pytz.timezone(TZ)
    local = execution_dt.astimezone(tz)
    return local.weekday() == 4  # Fri

default_args = {"owner": "GlazkovOI", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id=DAG_ID,
    start_date=dt.datetime(2025, 1, 1, tzinfo=pytz.UTC),
    schedule=SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["i38","nextcloud","minio","iceberg","spark","telegram"],
) as dag:

    @task
    def sync_nextcloud_to_minio() -> List[str]:
        client = get_webdav_client()
        s3 = get_s3()

        base_dir = WEBDAV_REMOTE_DIR if WEBDAV_REMOTE_DIR.endswith("/") else WEBDAV_REMOTE_DIR + "/"
        names = client.list(base_dir)

        uploaded, skipped = [], []

        for name in names:
            if name.endswith("/") or not name.lower().endswith(".csv"):
                continue

            remote_path = name if name.startswith(base_dir) else base_dir + name
            filename = os.path.basename(remote_path)
            s3_key = f"{LANDING_PREFIX}/{filename}"

            # 1) Читаем метаданные файла на Nextcloud (без скачивания файла)
            try:
                info = client.resource(remote_path).info() or {}
            except WebDavException:
                # если из-за прав/символов не смогли — пробуем старым способом
                info = {}

            nc_size = int(info.get("size")) if info.get("size") is not None else None
            nc_etag = (info.get("etag") or "").strip('"')
            nc_modified = (info.get("modified") or "")

            # 2) Смотрим, есть ли уже объект в MinIO и совпадают ли метаданные
            try:
                head = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                meta = head.get("Metadata", {})
                same_etag = bool(nc_etag) and meta.get("nc_etag") == nc_etag
                same_size_and_mtime = (
                    nc_size is not None and head.get("ContentLength") == nc_size and meta.get("nc_modified") == nc_modified
                )
                if same_etag or same_size_and_mtime:
                    skipped.append(s3_key)
                    continue
            except ClientError as e:
                # если объекта нет — пойдём загружать; другие ошибки — пробрасываем
                code = e.response.get("Error", {}).get("Code")
                if code not in ("404", "NoSuchKey", "NotFound"):
                    raise

            # 3) Скачиваем и загружаем в MinIO с сохранением метаданных источника
            tmp = tempfile.NamedTemporaryFile(delete=False)
            tmp.close()
            client.download_sync(remote_path=remote_path, local_path=tmp.name)
            with open(tmp.name, "rb") as f:
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=f,
                    Metadata={
                        "nc_path": remote_path,
                        "nc_size": str(nc_size or 0),
                        "nc_modified": nc_modified,
                        "nc_etag": nc_etag,
                    },
                )
            os.unlink(tmp.name)
            uploaded.append(s3_key)

        print(f"Uploaded: {len(uploaded)}, skipped: {len(skipped)}")
        return uploaded

    landing_keys = sync_nextcloud_to_minio()

    # Инжест в Iceberg: SSH -> docker exec jupyter spark-submit …
    spark_ingest = SSHOperator(
        task_id="spark_ingest_to_iceberg",
        ssh_conn_id="ssh_spark_default",   # 192.168.150.29
        command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPT_INGEST_PATH} "
            f"{S3_BUCKET} {LANDING_PREFIX} {ICE_MAIN_TABLE} {ICE_LOG_TABLE} "
            f"'{CSV_SEP}' '{CSV_ENCODING}'"
        ),
        do_xcom_push=False,
        cmd_timeout=600,

    )
    @task
    def check_friday():
        # контекст ранa
        ctx = get_current_context()
        execution_date = ctx["execution_date"]
        dag_run = ctx.get("dag_run")

        # флаг: Variable I38_FORCE_SEND=1 или {"force_send": true} при ручном запуске
        force = Variable.get("I38_FORCE_SEND", default_var="0") == "1"
        if dag_run and isinstance(dag_run.conf, dict):
            force = force or bool(dag_run.conf.get("force_send"))

        if force:
            return True
        if not is_friday(execution_date):
            raise AirflowSkipException("Not Friday — skipping weekly export and telegram sending")
        return True

    friday_ok = check_friday()

    spark_export_weekly = SSHOperator(
        task_id="spark_export_weekly_per_subject",
        ssh_conn_id="ssh_spark_default",
        command=(
            f"{SPARK_SUBMIT_CMD} {SCRIPT_EXPORT_PATH} "
            f"{S3_BUCKET} {EXPORT_PREFIX} {ICE_MAIN_TABLE} "
            f"'{CSV_SEP}' '{CSV_ENCODING}' {EXPORT_DAYS}"
        ),
        do_xcom_push=False,
        cmd_timeout=SSH_CMD_TIMEOUT,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def zip_and_send_to_telegram():
        try:
            mapping_json = Variable.get(SUBJECT_CHAT_MAP_VAR)
            subject_chat_map: Dict[str, int] = json.loads(mapping_json)
        except Exception:
            subject_chat_map = DEFAULT_SUBJECT_CHAT_MAP

        if not subject_chat_map:
            print("SUBJECT_CHAT_MAP is empty — nothing to send.")
            return

        s3 = get_s3()
        token = get_telegram_token()
        if not token:
            raise RuntimeError("Telegram token not found in connection 'telegram_bot_sender'")

        sent, skipped = [], []
        for subject, chat_id in subject_chat_map.items():
            safe = make_slug(subject)
            key_prefix = f"{EXPORT_PREFIX}/{safe}.csv/"

            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=key_prefix)
            if "Contents" not in resp:
                skipped.append(subject); continue

            part_key = None
            for obj in resp["Contents"]:
                k = obj["Key"]
                if k.startswith(key_prefix) and "/part-" in k and k.endswith(".csv"):
                    part_key = k
                    break
            if not part_key:
                skipped.append(subject); continue

            csv_obj = s3.get_object(Bucket=S3_BUCKET, Key=part_key)
            csv_bytes = csv_obj["Body"].read()

            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{safe}.csv", csv_bytes)
            zip_buf.seek(0)

            url = f"https://api.telegram.org/bot{token}/sendDocument"
            files = {"document": (f"{safe}.zip", zip_buf, "application/zip")}
            data = {"chat_id": str(chat_id), "caption": f"i38 errors — {subject} (за последнюю неделю)"}
            r = requests.post(url, data=data, files=files, timeout=60)
            (sent if r.status_code == 200 else skipped).append(subject)

        print("Sent:", sent); print("Skipped:", skipped)
        return {"sent": sent, "skipped": skipped}

    # Граф
    landing_keys >> spark_ingest
    spark_ingest >> Label("Пятница?") >> friday_ok
    friday_ok >> spark_export_weekly >> zip_and_send_to_telegram()
