from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook

import clickhouse_connect


CH_CONN_ID = "ch_conn"
RAW_DB = "raw"
DEMO_DB = "demo"


def get_client():
    """
    Подключение берём из Airflow Connection ch_conn.

    Важно:
    - database в клиенте можно оставить любой (мы в запросах используем raw./demo.)
    - secure/verify можно задать в Extra:
        {"secure": true, "verify": true}
      или
        {"secure": true, "verify": false}
    """
    conn = BaseHook.get_connection(CH_CONN_ID)
    extra = conn.extra_dejson or {}

    host = conn.host
    port = conn.port or 8123
    username = conn.login or "default"
    password = conn.password or ""
    # database оставляем demo по умолчанию (не критично)
    database = conn.schema or DEMO_DB

    secure = bool(extra.get("secure", True))
    verify = extra.get("verify", True)

    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        secure=secure,
        verify=verify,
    )


def run_sql(sql: str) -> None:
    """
    Выполняем набор statement'ов. ClickHouse любит, когда команды по одной.
    """
    client = get_client()
    for stmt in [s.strip() for s in sql.split(";")]:
        if stmt:
            client.command(stmt)


DDL = f"""
CREATE DATABASE IF NOT EXISTS {DEMO_DB};

CREATE TABLE IF NOT EXISTS {DEMO_DB}.mart_orders
(
  order_id String,
  customer_id String,
  customer_unique_id String,
  customer_city String,
  customer_state String,

  order_status String,
  order_purchase_ts DateTime,
  order_purchase_date Date,
  order_approved_at Nullable(DateTime),
  order_delivered_customer_date Nullable(DateTime),
  order_estimated_delivery_date Nullable(DateTime),

  items_value Float64,
  freight_value Float64,
  payment_value Float64,

  items_cnt UInt32,
  sellers_cnt UInt32,

  review_score Nullable(UInt8),
  delivery_days Nullable(Int32),
  delivery_delay_days Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY (order_purchase_date, order_id);

CREATE TABLE IF NOT EXISTS {DEMO_DB}.mart_daily_kpis
(
  date Date,

  orders UInt32,
  delivered_orders UInt32,
  canceled_orders UInt32,

  revenue Float64,
  freight Float64,
  avg_order_value Float64,

  unique_customers UInt32,
  avg_review_score Nullable(Float64),

  avg_delivery_days Nullable(Float64),
  delivery_delay_rate Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY date;
"""


BUILD_MART_ORDERS = f"""
TRUNCATE TABLE {DEMO_DB}.mart_orders;

INSERT INTO {DEMO_DB}.mart_orders
WITH
  items AS (
    SELECT
      order_id,
      sum(price) AS items_value,
      sum(freight_value) AS freight_value,
      count() AS items_cnt,
      countDistinct(seller_id) AS sellers_cnt
    FROM {RAW_DB}.order_items_raw
    GROUP BY order_id
  ),
  pays AS (
    SELECT
      order_id,
      sum(payment_value) AS payment_value
    FROM {RAW_DB}.payments_raw
    GROUP BY order_id
  ),
  revs AS (
    SELECT
      order_id,
      avg(toFloat64(review_score)) AS avg_review_score
    FROM {RAW_DB}.reviews_raw
    GROUP BY order_id
  )
SELECT
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state,

  o.order_status,
  o.order_purchase_timestamp AS order_purchase_ts,
  toDate(o.order_purchase_timestamp) AS order_purchase_date,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,

  coalesce(items.items_value, 0.0) AS items_value,
  coalesce(items.freight_value, 0.0) AS freight_value,
  coalesce(pays.payment_value, 0.0) AS payment_value,

  toUInt32(coalesce(items.items_cnt, 0)) AS items_cnt,
  toUInt32(coalesce(items.sellers_cnt, 0)) AS sellers_cnt,

  toUInt8OrNull(revs.avg_review_score) AS review_score,

  if(
    o.order_delivered_customer_date IS NULL,
    NULL,
    dateDiff('day', o.order_purchase_timestamp, o.order_delivered_customer_date)
  ) AS delivery_days,

  if(
    o.order_delivered_customer_date IS NULL OR o.order_estimated_delivery_date IS NULL,
    NULL,
    dateDiff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date)
  ) AS delivery_delay_days
FROM {RAW_DB}.orders_raw o
LEFT JOIN {RAW_DB}.customers_raw c ON c.customer_id = o.customer_id
LEFT JOIN items ON items.order_id = o.order_id
LEFT JOIN pays ON pays.order_id = o.order_id
LEFT JOIN revs ON revs.order_id = o.order_id;
"""


BUILD_MART_DAILY = f"""
TRUNCATE TABLE {DEMO_DB}.mart_daily_kpis;

INSERT INTO {DEMO_DB}.mart_daily_kpis
SELECT
  order_purchase_date AS date,

  toUInt32(count()) AS orders,
  toUInt32(countIf(order_status = 'delivered')) AS delivered_orders,
  toUInt32(countIf(order_status = 'canceled')) AS canceled_orders,

  sum(items_value) AS revenue,
  sum(freight_value) AS freight,

  if(count() = 0, 0.0, sum(items_value) / count()) AS avg_order_value,

  toUInt32(countDistinct(customer_unique_id)) AS unique_customers,

  avgIf(toFloat64(review_score), review_score IS NOT NULL) AS avg_review_score,

  avgIf(toFloat64(delivery_days), delivery_days IS NOT NULL) AS avg_delivery_days,

  if(
    countIf(order_status = 'delivered' AND delivery_delay_days IS NOT NULL) = 0,
    NULL,
    countIf(order_status = 'delivered' AND delivery_delay_days > 0)
      / countIf(order_status = 'delivered' AND delivery_delay_days IS NOT NULL)
  ) AS delivery_delay_rate
FROM {DEMO_DB}.mart_orders
GROUP BY order_purchase_date
ORDER BY order_purchase_date;
"""


with DAG(
    dag_id="build_olist_marts_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # руками, чтобы удобно дебажить; потом можно поставить @daily
    catchup=False,
    tags=["olist", "clickhouse", "raw->demo", "mart"],
) as dag:

    @task
    def create_mart_tables():
        run_sql(DDL)

    @task
    def build_mart_orders():
        run_sql(BUILD_MART_ORDERS)

    @task
    def build_mart_daily_kpis():
        run_sql(BUILD_MART_DAILY)

    create_mart_tables() >> build_mart_orders() >> build_mart_daily_kpis()
