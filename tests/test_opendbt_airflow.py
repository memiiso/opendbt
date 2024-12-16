from airflow import DAG
from airflow.utils.dates import days_ago

from base_dbt_test import BaseDbtTest
from opendbt.airflow import OpenDbtAirflowProject, OpenDbtExecutorOperator


class TestOpenDbtProject(BaseDbtTest):

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
            p = OpenDbtAirflowProject(project_dir=self.DBTCORE_DIR, profiles_dir=self.DBTCORE_DIR, target='dev')
            p.load_dbt_tasks(dag=dag,
                             include_singular_tests=True,
                             include_dbt_seeds=True)

            for j in dag.tasks:
                # don't run the model we created to fail
                if 'my_failing_dbt_model' in j.task_id:
                    continue

                if isinstance(j, OpenDbtExecutorOperator):
                    # skip dbt tests which are triggering callbacks
                    j.command = "run" if j.command == "build" else j.command
                j.execute({})