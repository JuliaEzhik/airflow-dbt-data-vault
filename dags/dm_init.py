from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dm_init",
    start_date=days_ago(1),
    schedule_interval=None,   # run once manually
    catchup=False,
    tags=["dm","init","dbt"],
) as dag:
    ensure_dm_schema = SQLExecuteQueryOperator(
        task_id="ensure_dm_schema",
        conn_id="pg_dwh",  # change if your conn id differs
        sql="create schema if not exists dm;"
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )

    dbt_full_refresh_dm = BashOperator(
        task_id="dbt_full_refresh_dm",
        bash_command="cd /opt/airflow/dbt && dbt run --full-refresh --select dm.stat_orders",
    )

    create_indexes = SQLExecuteQueryOperator(
        task_id="create_indexes",
        conn_id="pg_dwh",
        sql="""
        create index if not exists ix_stat_orders_order_date on dm.stat_orders(order_date);
        """
    )

    ensure_dm_schema >> dbt_deps >> dbt_full_refresh_dm >> create_indexes