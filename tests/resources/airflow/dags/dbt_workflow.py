from pathlib import Path

from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from airflow import DAG
from opendbt.airflow import OpenDbtAirflowProject

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
        dag_id='dbt_workflow',
        default_args=default_args,
        description='DAG To run dbt',
        schedule_interval=None,
        start_date=days_ago(3),
        catchup=False,
        max_active_runs=1
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    DBTTEST_DIR = Path("/opt/dbttest")

    p = OpenDbtAirflowProject(project_dir=DBTTEST_DIR, profiles_dir=DBTTEST_DIR, target='dev')
    p.load_dbt_tasks(dag=dag, start_node=start, end_node=end)
