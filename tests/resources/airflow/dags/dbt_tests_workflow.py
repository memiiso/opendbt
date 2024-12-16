from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from opendbt.airflow import OpenDbtAirflowProject

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
        dag_id='dbt_tests_workflow',
        default_args=default_args,
        description='DAG To run dbt tests',
        schedule_interval=None,
        start_date=days_ago(3),
        catchup=False,
        max_active_runs=1
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    DBT_PROJ_DIR = Path("/opt/dbtcore")

    p = OpenDbtAirflowProject(project_dir=DBT_PROJ_DIR, profiles_dir=DBT_PROJ_DIR, target='dev')
    p.load_dbt_tasks(dag=dag, start_node=start, end_node=end, resource_type='test')
