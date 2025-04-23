import datetime
from datetime import timedelta

from airflow import DAG

from base_dbt_test import BaseDbtTest
from opendbt.airflow import OpenDbtAirflowProject


class TestOpenDbtProject(BaseDbtTest):

    def get_dag(self):
        return DAG(
            dag_id='dbt_test_workflow',
            schedule_interval=None,
            start_date=(datetime.datetime.today() - timedelta(days=10)),
            catchup=False,
            max_active_runs=1
        )

    def test_run_dbt_as_airflow_task(self):
        with self.get_dag() as dag:
            # load dbt jobs to airflow dag
            p = OpenDbtAirflowProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR, target='dev')
            p.load_dbt_tasks(dag=dag,
                             include_singular_tests=True,
                             include_dbt_seeds=True)

            for j in dag.tasks:
                if 'my_first_dbt_model' in j.task_id:
                    j.execute({})
                if 'my_executedlt_model' in j.task_id:
                    j.execute({})
                if 'my_executepython_model' in j.task_id:
                    j.execute({})
