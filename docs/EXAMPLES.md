# Example Use Cases

## Use customised adapter, provide jinja with further python methods

When you want to add more methods to existing adapter and make these methods available to jinja.
you can use `dbt_custom_adapter` variable and use your adapter class with dbt.

**Step-1:** Extend existing adapter, add new methods to it.
https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/opendbt/examples.py#L10-L26

**Step-2:** Edit `dbt_project.yml` file, set `dbt_custom_adapter` variable to the class name of your custom adapter.
```yml
vars:
  dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom
```

**Step-3:** Run dbt, now dbt is loading and using the provided adapter class.
```python
from opendbt import OpenDbtProject

dp = OpenDbtProject(project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir")
dp.run(command="run")
```

## Execute Python Model Locally

Using customized adapter and a custom materialization we can extend dbt to run local python code.
this is useful for the scenarios where data is imported from external API.

**Step-1:** Extend existing adapter, Add new adapter method which runs given python code.
Here we are extending DuckDBAdapter and adding new method `submit_local_python_job` to it. This method executes given
python code as a subprocess
https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/opendbt/examples.py#L10-L26

**Step-2:** Create materialization named `executepython`, In this materialization (from the jonja) we call this new(
above) adapter method to run compiled python code

https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/opendbt/macros/executepython.sql#L1-L26

**Step-3:** Creating a sample python model using the `executepython` materialization. which is executed locally by dbt.

https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/tests/resources/dbttest/models/my_executepython_dbt_model.py#L1-L22

## Enable Model-Level Orchestration Using Airflow

**Step-1:** Create Dag to run dbt project
https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/tests/resources/airflow/dags/dbt_workflow.py#L17-L32

![airflow-dbt-flow.png](assets%2Fairflow-dbt-flow.png)

#### Create dag using subset of dbt models

```python
from opendbt.airflow import OpenDbtAirflowProject

# create dbt build tasks for models with given tag
p = OpenDbtAirflowProject(resource_type='model', project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir",
                          target='dev', tag="MY_TAG")
p.load_dbt_tasks(dag=dag, start_node=start, end_node=end)
```

#### Create dag to run tests

```python
from opendbt.airflow import OpenDbtAirflowProject

# create dbt test tasks with given model tag
p = OpenDbtAirflowProject(resource_type='test', project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir",
                          target='dev', tag="MY_TAG")
p.load_dbt_tasks(dag=dag, start_node=start, end_node=end)
```

## Create page on Airflow Server to serve DBT docs

While its very practical to use airflow for dbt executions, it could also be used to server dbt docs.

here is how:
**Step-1:** Create python file under airflow `/{airflow}/plugins` directory, with following code.
Adjust the given path to the folder where dbt docs are published.

https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/tests/resources/airflow/plugins/airflow_dbtdocs_page.py#L1-L6

**Step-2:** Restart airflow, and check that new link `DBT Docs` is created.
![airflow-dbt-docs-link.png](assets%2Fairflow-dbt-docs-link.png)

**Step-3:** open the link and browse dbt docs.
![airflow-dbt-docs-page.png](assets%2Fairflow-dbt-docs-page.png)