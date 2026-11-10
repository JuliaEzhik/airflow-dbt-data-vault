from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dm_build",
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # hourly (change if you want daily)
    catchup=False,
    tags=["dm","dbt"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps",
    )

    dbt_run_dm = BashOperator(
        task_id="dbt_run_dm",
        bash_command="cd /opt/airflow/dbt && dbt run --select dm.stat_orders",
    )

    dbt_test_dm = BashOperator(
        task_id="dbt_test_dm",
        bash_command="cd /opt/airflow/dbt && dbt test --select dm.stat_orders",
    )

    dbt_deps >> dbt_run_dm >> dbt_test_dm