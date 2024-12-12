from pathlib import Path
from unittest import TestCase

from airflow import DAG
from airflow.utils.dates import days_ago

from opendbt.airflow import OpenDbtAirflowProject


class TestOpenDbtProject(TestCase):
    RESOURCES_DIR = Path(__file__).parent.joinpath("resources")
    DBTTEST_DIR = RESOURCES_DIR.joinpath("dbttest")

    def get_dag(self):
        return DAG(
            dag_id='dbt_test_workflow',
            schedule_interval=None,
            start_date=days_ago(3),
            catchup=False,
            max_active_runs=1
        )

    def test_run_dbt_as_airflow_task(self):
        with self.get_dag() as dag:
            # load dbt jobs to airflow dag
            p = OpenDbtAirflowProject(project_dir=self.DBTTEST_DIR, profiles_dir=self.DBTTEST_DIR, target='dev')
            p.load_dbt_tasks(dag=dag,
                             include_singular_tests=True,
                             include_dbt_seeds=True)

            for j in dag.tasks:
                # don't run the model we created to fail
                if 'my_failing_dbt_model' in j.task_id:
                    continue
                # run all dbt tasks trough airflow operator
                j.execute({})

