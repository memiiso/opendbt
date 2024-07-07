# Example Use Cases

## Use customised adapter, provide jinja with further python methods

When you want to add more methods to existing adapter and make this methods available to jinja.
you can use `dbt_custom_adapter` variable and provide your adapter class.

**Step-1:** Extend existing adapter
https://github.com/memiiso/opendbt/blob/ddf0b1bac379aa42961900f35dfc9938b1bc19c4/opendbt/examples.py#L10-L26

**Step-2:** Edit `dbt_project.yml` file, set `dbt_custom_adapter` variable to the class name of the new adapter.
```yml
vars:
  dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom
```

**Step-3:** Run dbt, nwo dbt uses the provided adapter class.
```python
from opendbt import OpenDbtProject

dp = OpenDbtProject(project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir")
dp.run(command="run")
```

## Execute Python Model Locally

Using customized adapter and a custom materialization we can extend dbt to run local python code.
this is useful for the scenarios where data is imported from external API.

**Step-1:** Extend existing adapter, Add new adapter method which runs given python code.

https://github.com/memiiso/opendbt/blob/ddf0b1bac379aa42961900f35dfc9938b1bc19c4/opendbt/examples.py#L10-L26

**Step-2:** Create materialization, where from the jonja we call this new adapter method

https://github.com/memiiso/opendbt/blob/ddf0b1bac379aa42961900f35dfc9938b1bc19c4/opendbt/macros/executepython.sql#L1-L26

**Step-3:** Create model using the materialization

https://github.com/memiiso/opendbt/blob/ddf0b1bac379aa42961900f35dfc9938b1bc19c4/tests/resources/dbttest/models/my_executepython_dbt_model.py#L1-L22

## Enable Model-Level Orchestration Using Airflow

**Step-1:** Create Dag to run dbt project
https://github.com/memiiso/opendbt/blob/ddf0b1bac379aa42961900f35dfc9938b1bc19c4/tests/resources/airflow/dags/dbt_workflow.py#L17-L32

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

While its very practical to use airflow for dbt executions, it can be also used to server dbt docs.

here is how:
**Step-1:** Create python file under airflow `/{airflow}/plugins` directory, with following code.
Adjust the given path to the folder where dbt docs are published

https://github.com/memiiso/opendbt/blob/154b3e26981d157da70ebb98f1a1576f1fa55832/tests/resources/airflow/plugins/airflow_dbtdocs_page.py#L1-L6

**Step-2:** Restart airflow, and check that new link `DBT Docs` is created.
![airflow-dbt-docs-link.png](assets%2Fairflow-dbt-docs-link.png)

**Step-3:** open the link and browse dbt docs
![airflow-dbt-docs-page.png](assets%2Fairflow-dbt-docs-page.png)