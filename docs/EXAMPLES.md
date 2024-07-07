# Example Use Cases

## Use customised adapter, provide jinja with custom python methods

When you want to add more methods to existing adapter and make this methods available to jinja.
you can use `dbt_custom_adapter` variable and provide your adapter class.

Example:
Extend existing adapter:
https://github.com/ismailsimsek/opendbt/blob/dd11150c1181b3488d123c7a0eeab7a3712ccf59/opendbt/examples.py#L10-L26

set `dbt_custom_adapter` variable in your `dbt_project.yml` file

```yml
vars:
  dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom
```

Run it:

```python
from opendbt import OpenDbtProject

dp = OpenDbtProject(project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir")
dp.run(command="run")
```

## Execute Python Model Locally

Using customized adapter and a custom materialization we can extend dbt to run local python code.
this is useful for the scenarios where data is imported from external API.

Extend existing adapter: add new adapter method which runs given python code.

https://github.com/ismailsimsek/opendbt/blob/dd11150c1181b3488d123c7a0eeab7a3712ccf59/opendbt/examples.py#L10-L26

Create materialization to use this method

https://github.com/ismailsimsek/opendbt/blob/dd11150c1181b3488d123c7a0eeab7a3712ccf59/opendbt/macros/executepython.sql#L1-L26

create model which runs local python and materialize the data using new materialization

https://github.com/ismailsimsek/opendbt/blob/dd11150c1181b3488d123c7a0eeab7a3712ccf59/tests/resources/dbttest/models/my_executepython_dbt_model.py#L1-L999

## Enable Model-Level Orchestration Using Airflow
@TODO

![airflow-dbt-flow.png](assets%2Fairflow-dbt-flow.png)



