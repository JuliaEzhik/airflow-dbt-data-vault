"""Hourly pipeline: generate a few new raw orders, then rebuild Data Vault + DM with dbt."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

CONN_ID = "pg_dwh"


def generate_new_orders(num_rows_min: int = 3, num_rows_max: int = 7) -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    statuses = ["placed", "shipped", "delivered"]
    now = datetime.now(timezone.utc)
    num = random.randint(num_rows_min, num_rows_max)

    rows = []
    for _ in range(num):
        order_id = str(uuid.uuid4())
        order_ts = now
        customer_id = random.randint(1, 10)
        amount = round(random.uniform(5.0, 500.0), 2)
        status = random.choice(statuses)
        updated_at = now
        rows.append((order_id, order_ts, customer_id, amount, status, updated_at))

    insert_sql = (
        "insert into raw.orders "
        "(order_id, order_ts, customer_id, order_amount, status, updated_at) "
        "values (%s, %s, %s, %s, %s, %s)"
    )

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()


with DAG(
    dag_id="orders_to_dv",
    start_date=days_ago(1),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["demo", "incremental", "dbt", "data-vault"],
) as dag:
    generate_orders = PythonOperator(
        task_id="generate_new_orders",
        python_callable=generate_new_orders,
        op_kwargs={"num_rows_min": 3, "num_rows_max": 7},
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )

    dbt_run_dv = BashOperator(
        task_id="dbt_run_dv",
        bash_command="cd /opt/airflow/dbt && dbt run --select +dv",
    )

    dbt_run_dm = BashOperator(
        task_id="dbt_run_dm",
        bash_command="cd /opt/airflow/dbt && dbt run --select dm.stat_orders",
    )

    generate_orders >> dbt_deps >> dbt_run_dv >> dbt_run_dm
