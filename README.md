[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
# opendbt

The `opendbt` library extends the capabilities of dbt. It unlocks many customizations, allowing you to tailor dbt to
your specific needs and data workflows.

Forexample creating custom transformations by customizing existing adapters.

With `opendbt` you can go beyond the core functionalities of dbt by seamlessly integrating your customized adapter and
provide jinja with further adapter/python methods.

# Example use cases

- Use customised adapter, provide jinja with additional custom python methods added to adapter
- Execute Python model(Python code) locally
- Enable Granular Model-Level Orchestration Using Airflow
- Create page on Airflow Server to serve DBT docs as a page of airflow server

For more details please see [examples](docs/EXAMPLES.md).

## Installation

install from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/master.zip --upgrade --user
```

install version from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/refs/tags/0.2.0.zip --upgrade --user
```

### Contributors

<a href="https://github.com/memiiso/opendbt/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/opendbt" />
</a>
