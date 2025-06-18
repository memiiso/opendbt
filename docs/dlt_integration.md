# Using DLT within DBT model

## 1: Local Python Execution

Ensure you are able to run local python model.

## 2: Create DLT Model

To integrate DLT (Data Load Tool) into your dbt model, you'll first create a DLT pipeline. Key parameters for this pipeline, such as the destination, dataset_name, and table_name, can be dynamically configured using dbt variables.

You'll then need to define a custom method responsible for ingesting the source data that feeds into the DLT pipeline. The example code demonstrates this with an events() method.

It's important to note that the DLT destination is typically derived from your existing dbt connection profile. While the specific construction of the DLT destination might vary depending on the target database (e.g., Snowflake, BigQuery, DuckDB), your dbt connection provides the necessary details to establish it in all scenarios. This allows for a consistent approach to configuring DLT pipelines within your dbt projects.

```python
--8<-- "tests/resources/dbtcore/models/my_executedlt_model.py"
```
