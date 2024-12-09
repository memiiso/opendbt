![logo-badge](https://github.com/mac-s-g/github-help-wanted/blob/master/src/images/logo-full.png?raw=true)
[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)

# opendbt

This project adds new capabilities to dbt-core by dynamically modifying dbt's source code.
dbt is a popular solution for batch data processing in data analytics. While it operates on
an [open-core model](https://opencoreventures.com/blog/2023-07-open-core-is-misunderstood-business-model/), which can
sometimes limit the inclusion of community features in the open-source version. no worries opendbt is here to solve it.
opendbt offers a fully open-source package to address these concerns. **OpenDBT builds upon dbt-core, adding valuable
features without changing dbt-core code.**

`opendbt` unlocks many customization which are not in dbt-core, allowing end user to tailor dbt to his/her specific
needs and data workflows.

With `opendbt` you can go beyond the core functionalities of dbt. For example seamlessly integrating your customized
adapter and providing jinja context with further adapter/python methods.

# Features

- Customize Existing Adapters: Leverage OOP to create custom adapters that extends existing ones. With this user
  could provide more python features to jinja
- Execute Local Python Code: Use dbt Python models to run local Python code. For example, you could import data from web
  APIs directly within your dbt model.
- Granular Model-Level Orchestration with Airflow: Integrate Airflow for fine-grained control over model execution.
- Serve dbt Docs in Airflow UI: Create a custom page on the Airflow server that displays dbt documentation as an Airflow
  UI page.
- Customized dbt Docs: Replace the default dbt documentation page with your own custom index.html file.
- Run end to end ETL pipeline with dbt
  using [dlt](https://dlthub.com/) [integration](https://github.com/memiiso/opendbt/issues/40)

For detailed examples, see: [examples](docs/EXAMPLES.md).

![opendbt-airflow-ui.png](docs/assets/opendbt-airflow-ui.png)

## Installation

install version from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/refs/tags/0.4.0.zip --upgrade --user
```

## **Your Contributions Matter**

The project completely open-source, using the Apache 2.0 license.
opendbt still is a young project and there are things to improve.
Please feel free to test it, give feedback, open feature requests or send pull requests.

### Contributors

<a href="https://github.com/memiiso/opendbt/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/opendbt" />
</a>
