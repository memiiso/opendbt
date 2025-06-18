# Executing Python Models Locally

You can extend dbt to execute Python code locally by utilizing a customized adapter in conjunction with a custom materialization.

This approach offers a powerful way to integrate tasks like data ingestion from external APIs directly into your dbt workflows. It allows for the development of end-to-end data ingestion pipelines entirely within the dbt framework.

:warning:NOTE: Be aware that this method means Python code execution and data processing occur within the dbt environment itself. For instance, if dbt is deployed on an Airflow server, the processing happens on that server. While this is generally fine for handling reasonable amounts of data, it might not be suitable for very large datasets or resource-intensive processing tasks.

## 1: Extend Adapter

Opendbt already comes with this feature implemented!

Below `submit_local_python_job` method will execute the provided Python code(compiled Python model) as a subprocess.
**Note the `connection` variable passed down to the python model. Which is used to save data to destination database.**

```python hl_lines="40-48"
--8<-- "opendbt/dbt/shared/adapters/impl.py"
```

## 2: Pyhon Execution from macro
Create a new materialization named `executepython`. This materialization will call the newly added
`submit_local_python_job` method to execute the compiled Python code.

```jinja hl_lines="2"
--8<-- "opendbt/macros/executepython.sql:14:16"
```

## 3: Final
Let's create a sample Python model that will be executed locally by dbt using the `executepython`
materialization.

```python
--8<-- "tests/resources/dbtcore/models/my_executepython_model.py"
```