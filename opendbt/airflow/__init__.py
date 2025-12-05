from datetime import timedelta
from pathlib import Path
from typing import Tuple

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator

import opendbt


# pylint: disable=too-many-instance-attributes
class OpenDbtExecutorOperator(BaseOperator):
    """
    An Airflow operator for executing dbt commands.

    Supports Jinja templating for the following fields:
    - args: list of command-line arguments (e.g., ['--vars', '{"key": "{{ ds }}"}'])
    - select: model selection string (e.g., "{{ var.value.model_name }}")
    - target: dbt target (e.g., "{{ var.value.environment }}")
    - command: dbt command to run (e.g., "{{ var.value.dbt_command }}")
    """

    template_fields = ('args', 'select', 'target', 'command')

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
        self.select = select
        self.use_subprocess = use_subprocess
        self.args = list(args) if args else []

        # use separate colour for test and other executions
        if self.command == "test":
            self.ui_color = "#1CB1C2"
        else:
            self.ui_color = "#0084ff"

    def execute(self, context):
        """
        Execute the dbt command.
        """
        # Build final args list after templating has been applied
        run_args = list(self.args) if self.args else []
        if self.select:
            run_args += ["--select", self.select]

        runner = opendbt.OpenDbtProject(project_dir=self.project_dir,
                                        profiles_dir=self.profiles_dir,
                                        target=self.target)
        runner.run(command=self.command, args=run_args, use_subprocess=self.use_subprocess)


# pylint: disable=too-many-locals, too-many-branches
class OpenDbtAirflowProject(opendbt.OpenDbtProject):

    def _create_test_operator(self, dag: DAG, test_type: str, tag: str = None) -> OpenDbtExecutorOperator:
        """
        Create a test operator with optional tag filtering.

        Parameters:
        dag (DAG): The Airflow DAG object.
        test_type (str): The type of test ('singular' or 'generic').
        tag (str, optional): The tag to filter tests by.

        Returns:
        OpenDbtExecutorOperator: The test operator.
        """
        select_str = f"test_type:{test_type}"
        if tag:
            select_str = f"tag:{tag},test_type:{test_type}"

        return OpenDbtExecutorOperator(
            dag=dag,
            task_id=f"{self.project_dir.name}_{test_type}_tests",
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir,
            target=self.target,
            command="test",
            select=select_str,
            args=self.args
        )

    def _connect_test_nodes(self, dbt_tasks: dict, test_operator: OpenDbtExecutorOperator,
                           end_node: BaseOperator):
        """
        Connect test operator between leaf tasks and end node.

        Parameters:
        dbt_tasks (dict): Dictionary of dbt tasks.
        test_operator (OpenDbtExecutorOperator): The test operator to insert.
        end_node (BaseOperator): The end node of the DAG.
        """
        test_operator.set_downstream(end_node)
        for _, task in dbt_tasks.items():
            if end_node in task.downstream_list:
                task.downstream_task_ids.remove(end_node.task_id)
                task.set_downstream(test_operator)


    def load_dbt_tasks(self,
                       dag: DAG,
                       start_node: BaseOperator = None,
                       end_node: BaseOperator = None,
                       tag: str = None,
                       resource_type="all",
                       include_dbt_seeds=False,
                       include_singular_tests=False,
                       run_tests_after_all_models=False) -> Tuple[BaseOperator, BaseOperator]:
        """
        This method is used to add dbt tasks to Given DAG.

        Parameters:
        dag (DAG): The Airflow DAG object where the dbt tasks will be added.
        start_node (BaseOperator, optional): The starting node of the DAG. If not provided, an EmptyOperator will be used.
        end_node (BaseOperator, optional): The ending node of the DAG. If not provided, an EmptyOperator will be used.
        tag (str, optional): The tag to filter the dbt tasks. If provided, only tasks with this tag will be added to the DAG.
        resource_type (str, optional): The type of dbt resource to run. It can be "all", "model", or "test". Default is "all".
        include_dbt_seeds (bool, optional): A flag to indicate whether to run dbt seeds before all other dbt jobs. Default is False.
        include_singular_tests (bool, optional): A flag to indicate whether to run singular tests after all other tasks. Default is False.
        run_tests_after_all_models (bool, optional): A flag to run generic tests after all models are built.
            When True, models use 'run' instead of 'build', and all generic tests run in a final step. Default is False.

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
                                                 command="seed",
                                                 args=self.args
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
                                                                        select=node.name,
                                                                        args=self.args
                                                                        )
                if node.resource_type == "model":
                    dbt_tasks[node.unique_id] = EmptyOperator(dag=dag, task_id=node.unique_id)

            if node.resource_type == "model" and resource_type in ["all", "model"]:
                # NOTE `build` command also runs the tests that's why are skipping tests for models below by default
                model_command = "run" if run_tests_after_all_models else "build"
                dbt_tasks[node.unique_id] = OpenDbtExecutorOperator(dag=dag,
                                                                    task_id=node.unique_id,
                                                                    project_dir=self.project_dir,
                                                                    profiles_dir=self.profiles_dir,
                                                                    target=self.target,
                                                                    command=model_command,
                                                                    select=node.name,
                                                                    args=self.args
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
                                                                    select=node.name,
                                                                    args=self.args
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
            singular_tests = self._create_test_operator(dag, "singular", tag)

        generic_tests = None
        if run_tests_after_all_models:
            generic_tests = self._create_test_operator(dag, "generic", tag)

        for _, task in dbt_tasks.items():
            if not task.downstream_task_ids:
                # set downstream dependencies for the end nodes.
                self.log.debug(f"Setting downstream  of {task.task_id} -> {end_node.task_id}")
                task.set_downstream(end_node)

            if not task.upstream_task_ids:
                # set upstream dependencies for the nodes which don't have upstream dependency
                self.log.debug(f"Setting upstream  of {task.task_id} -> {start_node}")
                task.set_upstream(start_node)

        # Connect test nodes independently - they run in parallel after all model tasks
        # We need to insert test nodes between leaf model tasks and end_node
        if run_tests_after_all_models and generic_tests:
            self._connect_test_nodes(dbt_tasks, generic_tests, end_node)

        if include_singular_tests and singular_tests:
            self._connect_test_nodes(dbt_tasks, singular_tests, end_node)

        return start_node, end_node
