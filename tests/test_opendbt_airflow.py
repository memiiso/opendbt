from pathlib import Path
from unittest import TestCase

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from opendbt.airflow import OpenDbtAirflowProject


class TestOpenDbtProject(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")
    TEST_DAG = DAG(
        dag_id='dbt_test_workflow',
        schedule_interval=None,
        start_date=days_ago(3),
        catchup=False,
        max_active_runs=1
    )

    def test_run_dbt_as_airflow_task(self):
        with self.TEST_DAG as dag:
            # load dbt jobs to airflow dag
            start = EmptyOperator(task_id="start")
            end = EmptyOperator(task_id="end")
            p = OpenDbtAirflowProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR, target='dev')
            p.load_dbt_tasks(dag=dag, start_node=start, end_node=end, include_singular_tests=True,
                             include_dbt_seeds=True)

            for j in dag.tasks:
                # run all dbt tasks trough airflow operator
                j.execute({})
