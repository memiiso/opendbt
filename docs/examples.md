# Examples

## Orchestrating dbt Models with Airflow

**Step-1:** Let's create an Airflow DAG to orchestrate the execution of your dbt project.

```python
--8<-- "tests/resources/airflow/dags/dbt_workflow.py:17:32"
```

![airflow-dbt-flow.png](assets%2Fairflow-dbt-flow.png)

#### Creating Airflow DAG that selectively executes a specific subset of models from your dbt project.

```python
from opendbt.airflow import OpenDbtAirflowProject

# create dbt build tasks for models with given tag
p = OpenDbtAirflowProject(resource_type='model', project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir",
                          target='dev', tag="MY_TAG")
p.load_dbt_tasks(dag=dag, start_node=start, end_node=end)
```

#### Creating dag to run dbt tests

```python
from opendbt.airflow import OpenDbtAirflowProject

# create dbt test tasks with given model tag
p = OpenDbtAirflowProject(resource_type='test', project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir",
                          target='dev', tag="MY_TAG")
p.load_dbt_tasks(dag=dag, start_node=start, end_node=end)
```

## Integrating dbt Documentation into Airflow

Airflow, a powerful workflow orchestration tool, can be leveraged to streamline not only dbt execution but also dbt
documentation access. By integrating dbt documentation into your Airflow interface, you can centralize your data
engineering resources and improve team collaboration.

here is how:
**Step-1:** Create python file. Navigate to your Airflow's `{airflow}/plugins` directory.
Create a new Python file and name it appropriately, such as `dbt_docs_plugin.py`. Add following code to
`dbt_docs_plugin.py` file.
Ensure that the specified path accurately points to the folder where your dbt project generates its documentation.
https://github.com/memiiso/opendbt/blob/a5a7a598a3e4f04e184b38257578279473d78cfc/tests/resources/airflow/plugins/airflow_dbtdocs_page.py#L1-L6

**Step-2:** Restart Airflow to activate the plugin. Once the restart is complete, you should see a new link labeled
`DBT Docs` within your Airflow web interface. This link will provide access to your dbt documentation.
![airflow-dbt-docs-link.png](assets%2Fairflow-dbt-docs-link.png)

**Step-3:** Click on the `DBT Docs` link to open your dbt documentation.
![airflow-dbt-docs-page.png](assets%2Fairflow-dbt-docs-page.png)

**Step-4:** To use UI of Data Catalog (Demo), run command and reload the page:
```
python -m opendbt docs generate
```
![docs-lineage.png](assets%2Fdocs-lineage.png)