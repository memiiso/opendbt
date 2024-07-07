[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
# opendbt

The `opendbt` library extends the capabilities of dbt. It unlocks many customizations, allowing you to tailor dbt to
your specific needs and data workflows.

Forexample create custom transformations by customizing existing adapters

With `opendbt` you can go beyond the core functionalities of dbt by seamlessly integrating your customized adapter and
providing jinja with
custom Python methods tailored to your advanced needs.

# Sample use cases, examples

- Use customised adapter, provide jinja with custom python methods
- Execute Python Model(Python code) Locally
- Enable Model-Level Orchestration Using Airflow

please see [examples](docs/EXAMPLES.md).

## Installation

install from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/master.zip --upgrade --user
```

install version from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/refs/tags/0.1.0.zip --upgrade --user
```

### Contributors

<a href="https://github.com/memiiso/opendbt/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/opendbt" />
</a>