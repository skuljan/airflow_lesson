from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

import clickhouse_connect


def clickhouse_ping():
    # Берём параметры из Airflow Connection ch_conn
    conn = BaseHook.get_connection("ch_conn")

    # Важно:
    # - host: conn.host
    # - port: conn.port (если не задан, подставим 8123)
    # - login/password: conn.login/conn.password
    #
    # Extra (опционально):
    #   {"secure": true, "verify": false, "interface": "http"}
    extra = conn.extra_dejson or {}

    host = conn.host
    port = conn.port or 8123
    username = conn.login or "default"
    password = conn.password or ""

    secure = bool(extra.get("secure", True))
    verify = extra.get("verify", False)

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        secure=secure,
        verify=verify,
    )

    result = client.query("SELECT 1 AS ok")
    # result.result_rows обычно [[1]]
    print("ClickHouse result rows:", result.result_rows)


with DAG(
    dag_id="ch_conn_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["clickhouse", "test"],
) as dag:
    PythonOperator(
        task_id="ping_clickhouse",
        python_callable=clickhouse_ping,
    )
