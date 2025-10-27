![logo-badge](https://github.com/mac-s-g/github-help-wanted/blob/master/src/images/logo-full.png?raw=true)
[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)

# opendbt

This project adds new capabilities to dbt-core by dynamically extending dbt's source code.

dbt is a popular solution for batch data processing in data analytics. While it operates on
an [open-core model](https://opencoreventures.com/blog/2023-07-open-core-is-misunderstood-business-model/), which can
sometimes limit the inclusion of community features in the open-source version. no worries opendbt is here to solve it.
opendbt offers a fully open-source package to address these concerns. **OpenDBT builds upon dbt-core, adding valuable
features without changing dbt-core code.**

With `opendbt` you can go beyond the core functionalities of dbt. For example seamlessly integrating your customized
adapter and providing jinja context with further adapter/python methods.

## Features

- :white_check_mark: Includes superior [dbt catalog UI](https://memiiso.github.io/opendbt/opendbtdocs/), user-friendly
  multi-project
  data catalog,
  including row level
  lineage, [see it here](https://memiiso.github.io/opendbt/opendbtdocs/)
- :white_check_mark: Integrates Python and DLT Jobs to dbt. Enables Extract&Load (EL) with dbt.
- :white_check_mark: Supports DBT Mesh setups. Supports running multiple projects which are using cross project ref
  models.
- :white_check_mark: And many more features, customization options.
  - Customize Existing Adapters: add your custom logic to current adapters
  - By extending current adapter provide more functions to jinja
  - Execute Local Python
    Code: [run local Python code](https://medium.com/@ismail-simsek/make-dbt-great-again-ec34f3b661f5). For example, you
    could import data from web APIs directly within your dbt model.
  - [Integrate DLT](https://github.com/memiiso/opendbt/issues/40). Run end to end ETL pipeline with dbt and DLT.
  - [Use multi project dbt-mesh setup cross-project references](https://docs.getdbt.com/docs/collaborate/govern/project-dependencies#how-to-write-cross-project-ref).
    - This feature was only available in "dbt Cloud Enterprise" so far.
  - Granular Model-Level Orchestration with Airflow: Integrate Airflow for fine-grained control over model execution.
  - Serve dbt Docs in Airflow UI: Create a custom page on the Airflow server that displays dbt documentation as an
    Airflow UI page. Supports both single-project and multi-project configurations with UI-based project switching.
  - Register [dbt callbacks](https://docs.getdbt.com/reference/programmatic-invocations#registering-callbacks) within a
    dbt project to trigger custom actions or alerting based on selected dbt events.

[See documentation for further details and detailed examples](https://memiiso.github.io/opendbt/).

![opendbt-airflow-ui.png](https://raw.githubusercontent.com/memiiso/opendbt/main/docs/assets/opendbt-airflow-ui.png)

## Installation

install from github or pypi:

```shell
pip install opendbt==0.13.0
# Or
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
