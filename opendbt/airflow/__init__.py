from datetime import timedelta
from pathlib import Path
from typing import Tuple

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator

import opendbt


class OpenDbtExecutorOperator(BaseOperator):
    """
    An Airflow operator for executing dbt commands.
    """

    def __init__(self,
                 project_dir: Path,
                 command: str,
                 target: str = None,
                 profiles_dir: Path = None,
                 select: str = None,
                 args: list = None,
                 # without using subprocess airflow randomly gets deadlock
                 use_subprocess: bool = True,
                 execution_timeout=timedelta(minutes=60), **kwargs) -> None:
        super().__init__(execution_timeout=execution_timeout, **kwargs)

        self.project_dir: Path = project_dir
        self.command = command
        self.profiles_dir: Path = profiles_dir
        self.target = target
        self.use_subprocess = use_subprocess
        self.args = args if args else []

        if select:
            self.args += ["--select", select]

        # use separate colour for test and other executions
        if self.command == "test":
            self.ui_color = "#1CB1C2"
        else:
            self.ui_color = "#0084ff"

    def execute(self, context):
        """
        Execute the dbt command.
        """
        runner = opendbt.OpenDbtProject(project_dir=self.project_dir,
                                        profiles_dir=self.profiles_dir,
                                        target=self.target)
        runner.run(command=self.command, args=self.args, use_subprocess=self.use_subprocess)


# pylint: disable=too-many-locals, too-many-branches
class OpenDbtAirflowProject(opendbt.OpenDbtProject):

    def load_dbt_tasks(self,
                       dag: DAG,
                       start_node: BaseOperator = None,
                       end_node: BaseOperator = None,
                       tag: str = None,
                       resource_type="all",
                       include_dbt_seeds=False,
                       include_singular_tests=False) -> Tuple[BaseOperator, BaseOperator]:
        """
        This method is used to add dbt tasks to Given DAG.

        Parameters:
        dag (DAG): The Airflow DAG object where the dbt tasks will be added.
        start_node (BaseOperator, optional): The starting node of the DAG. If not provided, an EmptyOperator will be used.
        end_node (BaseOperator, optional): The ending node of the DAG. If not provided, an EmptyOperator will be used.
        tag (str, optional): The tag to filter the dbt tasks. If provided, only tasks with this tag will be added to the DAG.
        resource_type (str, optional): The type of dbt resource to run. It can be "all", "model", or "test". Default is "all".
        run_dbt_seeds (bool, optional): A flag to indicate whether to run dbt seeds before all other dbt jobs. Default is False.

        Returns:
        Tuple[BaseOperator, BaseOperator]: The start and end nodes of the DAG after adding the dbt tasks.
        """

        start_node = start_node if start_node else EmptyOperator(task_id='dbt-%s-start' % self.project_dir.name,
                                                                 dag=dag)
        end_node = end_node if end_node else EmptyOperator(task_id='dbt-%s-end' % self.project_dir.name, dag=dag)

        if include_dbt_seeds:
            # add dbt seeds job after start node abd before all other dbt jobs
            first_node = start_node
            start_node = OpenDbtExecutorOperator(dag=dag,
                                                 task_id="dbt-seeds",
                                                 project_dir=self.project_dir,
                                                 profiles_dir=self.profiles_dir,
                                                 target=self.target,
                                                 command="seed"
                                                 )
            start_node.set_upstream(first_node)

        manifest = self.manifest()
        dbt_tasks = {}
        # create all the jobs. granular as one job per model/table
        for key, node in manifest.nodes.items():
            if tag and tag not in node.tags:
                self.log.debug(
                    f"Skipping node:{node.name} because it dont have desired desired-tag={tag} node-tags={node.tags}")
                # LOG DEBUG OR TRACE here print(f" tag:{tag}  NOT in {node.tags} SKIPP {node.name}")
                continue  # skip if the node don't have the desired tag

            if resource_type == "test" and not str(node.name).startswith("source_"):
                if node.resource_type == "test":
                    dbt_tasks[node.unique_id] = OpenDbtExecutorOperator(dag=dag,
                                                                        task_id=node.unique_id,
                                                                        project_dir=self.project_dir,
                                                                        profiles_dir=self.profiles_dir,
                                                                        target=self.target,
                                                                        command="test",
                                                                        select=node.name
                                                                        )
                if node.resource_type == "model":
                    dbt_tasks[node.unique_id] = EmptyOperator(dag=dag, task_id=node.unique_id)

            if node.resource_type == "model" and resource_type in ["all", "model"]:
                # NOTE `build` command also runs the tests that's why are skipping tests for models below
                dbt_tasks[node.unique_id] = OpenDbtExecutorOperator(dag=dag,
                                                                    task_id=node.unique_id,
                                                                    project_dir=self.project_dir,
                                                                    profiles_dir=self.profiles_dir,
                                                                    target=self.target,
                                                                    command="build",
                                                                    select=node.name
                                                                    )

            if node.resource_type == "test" and str(node.name).startswith("source_") and resource_type in ["all",
                                                                                                           "test"]:
                # we are skipping model tests because they are included above with model execution( `build` command)
                # source table tests
                dbt_tasks[node.unique_id] = OpenDbtExecutorOperator(dag=dag,
                                                                    task_id=node.unique_id,
                                                                    project_dir=self.project_dir,
                                                                    profiles_dir=self.profiles_dir,
                                                                    target=self.target,
                                                                    command="test",
                                                                    select=node.name
                                                                    )

        # set upstream dependencies using dbt dependencies
        for key, node in manifest.nodes.items():
            if tag and tag not in node.tags:
                continue  # skip if the node don't have the desired tag
            if node.unique_id in dbt_tasks:  # node.resource_type == "model" or True or
                task = dbt_tasks[node.unique_id]
                if node.depends_on_nodes:
                    for upstream_id in node.depends_on_nodes:
                        if upstream_id in dbt_tasks:
                            self.log.debug(f"Setting upstream  of {task.task_id} -> {upstream_id}")
                            task.set_upstream(dbt_tasks[upstream_id])

        singular_tests = None
        if include_singular_tests:
            singular_tests = OpenDbtExecutorOperator(dag=dag,
                                                     task_id=f"{self.project_dir.name}_singular_tests",
                                                     project_dir=self.project_dir,
                                                     profiles_dir=self.profiles_dir,
                                                     target=self.target,
                                                     command="test",
                                                     select="test_type:singular"
                                                     )
        for k, task in dbt_tasks.items():
            if not task.downstream_task_ids:
                # set downstream dependencies for the end nodes.
                self.log.debug(f"Setting downstream  of {task.task_id} -> {end_node.task_id}")

                if include_singular_tests and singular_tests:
                    task.set_downstream(singular_tests)
                else:
                    task.set_downstream(end_node)

            if not task.upstream_task_ids:
                # set upstream dependencies for the nodes which don't have upstream dependency
                self.log.debug(f"Setting upstream  of {task.task_id} -> {start_node}")
                task.set_upstream(start_node)

        if include_singular_tests:
            singular_tests.set_downstream(end_node)
        return start_node, end_node
