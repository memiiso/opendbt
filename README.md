![logo-badge](https://github.com/mac-s-g/github-help-wanted/blob/master/src/images/logo-full.png?raw=true)
[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)

### Enhancing dbt with Python Runtime Patching

This project leverages Python to extend dbt's core functionality. By dynamically modifying dbt's source code, we're
adding new capabilities.

**Your Contributions Matter**: We welcome your input. If you have a more efficient or elegant approach to achieving the
same results, please share it with us."

# opendbt

The `opendbt` library extends the capabilities of dbt. It unlocks many customizations, allowing you to tailor dbt to
your specific needs and data workflows.

Forexample creating custom transformations by customizing existing adapters using object-oriented inheritance.

With `opendbt` you can go beyond the core functionalities of dbt by seamlessly integrating your customized adapter and
provide jinja with further adapter/python methods.

# Example use cases

- Create and use new adapter using OOP inheritance, provide jinja with additional custom python methods with your own adapter
- Execute Python model(Python code) locally
- Enable Granular Model-Level Orchestration Using Airflow
- Create page on Airflow Server to serve DBT docs as a new page under airflow UI
- Use customized dbt docs page, by providing custom index.html page

For more details please see [examples](docs/EXAMPLES.md).

## Installation

install from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/master.zip --upgrade --user
```

install version from github:

```shell
pip install https://github.com/memiiso/opendbt/archive/refs/tags/0.4.0.zip --upgrade --user
```

### Contributors

<a href="https://github.com/memiiso/opendbt/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/opendbt" />
</a>
