# Using a Custom Adapter

opendbt provides the flexibility to register and utilize custom adapters. This capability allows users to extend
existing adapters, add new methods, or override existing ones. By introducing new methods to an adapter, you can expose
them to dbt macros and leverage them within your dbt models, for instance, by calling a new method during
materialization.

Let's walk through the process step by step.

lets see it ste by step

## **1:** Extend Existing Adapter
Create a new adapter class that inherits from the desired base adapter. Add the necessary methods to this class.

here we are creating a method which will run given pyhon model locally. Notice `@available` decorator is making it
available for use in dbt macros.

```python
--8<-- "opendbt/examples.py:0:53"
```

## **2:** Activate Custom Adapter

In your `dbt_project.yml` file, set the `dbt_custom_adapter` variable to the fully qualified name of your
custom adapter class. when defined opendbt will take this adapter and activates it.

```yml
vars:
  dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom
```

Optionally you could provide this with run command

```python
from opendbt import OpenDbtProject

dp = OpenDbtProject(project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir",
                    args=['--vars', 'dbt_custom_adapter: opendbt.examples.DuckDBAdapterV2Custom'])
dp.run(command="run", args=['--select', 'my_executedlt_model'])
```

## **3:** Use new adapter in dbt macro

Call new adapter method from dbt macro/model.

```jinja hl_lines="2"
--8<-- "opendbt/macros/executepython.sql:14:16"
```

## **4:** Final

Execute dbt commands as usual. dbt will now load and utilize your custom adapter class, allowing you to
access the newly defined methods within your Jinja macros.

```python
from opendbt import OpenDbtProject

dp = OpenDbtProject(project_dir="/dbt/project_dir", profiles_dir="/dbt/profiles_dir")
dp.run(command="run")
```
