from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


CONN_ID = "pg_dwh"


def seed_initial_orders(num_rows: int = 20) -> None:
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    statuses = ["placed", "shipped", "delivered", "cancelled"]
    now = datetime.now(timezone.utc)

    rows = []
    for _ in range(num_rows):
        order_id = str(uuid.uuid4())
        # spread orders over past ~48 hours
        order_ts = now - timedelta(hours=random.randint(1, 48), minutes=random.randint(0, 59))
        customer_id = random.randint(1, 10)
        amount = round(random.uniform(10.0, 300.0), 2)
        status = random.choice(statuses)
        updated_at = order_ts
        rows.append((order_id, order_ts, customer_id, amount, status, updated_at))

    insert_sql = (
        "insert into raw.orders (order_id, order_ts, customer_id, order_amount, status, updated_at) "
        "values (%s, %s, %s, %s, %s, %s)"
    )

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="init_raw_orders",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "init"],
) as dag:
    create_schemas_and_table = PostgresOperator(
        task_id="create_schemas_and_table",
        postgres_conn_id=CONN_ID,
        sql=r"""
        create schema if not exists raw;
        create schema if not exists dv;
        create schema if not exists staging;

        create table if not exists raw.orders (
            order_id uuid primary key,
            order_ts timestamptz not null,
            customer_id int not null,
            order_amount numeric(10,2) not null,
            status text not null,
            updated_at timestamptz not null
        );

        create index if not exists idx_orders_order_ts on raw.orders(order_ts);
        create index if not exists idx_orders_updated_at on raw.orders(updated_at);
        """,
    )

    seed_data = PythonOperator(
        task_id="seed_initial_orders",
        python_callable=seed_initial_orders,
        op_kwargs={"num_rows": 25},
    )

    create_schemas_and_table >> seed_data


