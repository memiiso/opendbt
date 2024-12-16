from pathlib import Path

from airflow import DAG
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
        dag_id='dbt_mesh_workflow',
        default_args=default_args,
        description='DAG To run multiple dbt projects',
        schedule_interval=None,
        start_date=days_ago(3),
        catchup=False,
        max_active_runs=1
) as dag:
    DBT_PROJ_DIR = Path("/opt/dbtfinance")

    p = OpenDbtAirflowProject(project_dir=DBT_PROJ_DIR, profiles_dir=DBT_PROJ_DIR, target='dev')
    p.load_dbt_tasks(dag=dag, include_singular_tests=True, include_dbt_seeds=True)
