# Contributing Guide

Thanks for your interest in improving **OpenDBT**! This guide explains how to set up a development
environment, run the test-suite, and submit a change. If you bump into anything that is unclear, feel free
to open an issue or start a discussion – feedback helps us make this guide better.

---

## Before You Start

- Make sure you have **Python 3.8+** available.
- Familiarise yourself with the project goals by reading `README.md` and browsing the existing docs.
- Check the issue tracker to see whether someone else is already working on the problem you care about.

---

## Environment Setup

```bash
git clone https://github.com/<your-user>/opendbt.git
cd opendbt
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

The editable install gives you the command line entry point (`opendbt`) and all dependencies used in the
test suite (`.[test]`) and documentation tooling (`.[dev]`).

If you rely on optional integrations (for example, Airflow), install the relevant extras as well:

---

## Development Workflow

1. Create a topic branch off `main`:
   ```bash
   git checkout -b feat/<your-feature>
   ```
2. Make your changes in small, logical commits. Keep commit messages focused on the “what” and “why”.
3. Add or update tests that cover the behaviour you changed.
4. Run the test-suite (see below) and ensure it passes locally before opening a pull request.
5. Rebase on top of the latest `main` before submitting so the history stays clean.

---

## Running Tests

The project uses `pytest` to run a suite of unit and integration tests that wrap dbt invocations.

```bash
pytest
```

Useful options:

- Run a single test file: `pytest tests/test_catalog.py`
- Run a single test case: `pytest tests/test_catalog.py::TestCatalog::test_export`
- Show `print` output during tests: `pytest -s`

Some tests rely on Docker or external services. If you run into failures that look infrastructure related,
double-check the `tests/resources` configuration and ensure supporting services are available.

Linting is provided via `pylint`:

```bash
pylint opendbt
```

---

## Working With Documentation

All documentation lives inside the `docs/` directory and is served with MkDocs.

- Preview: `mkdocs serve` (the site will be available at http://127.0.0.1:8000/)
- Build static site: `mkdocs build`

If you add a new Markdown page, remember to update `mkdocs.yml` so the page appears in the navigation,
and keep pages written in clear, concise English.

---

## Manual Verification

Depending on your change, you may want to run additional manual checks:

- Validate `opendbt docs generate` on one of the example projects under `tests/resources`.
- For catalog-related updates, regenerate docs in `tests/resources/dbtcore` and open `target/index.html`
  with a simple `python -m http.server`.
- When modifying packaging metadata, run `python -m build` to ensure wheels and source distributions
  can be created successfully.

---

## Pull Request Checklist

Before you submit your PR:

- [ ] Tests are green locally (`pytest`).
- [ ] New or updated behaviour is covered by tests.
- [ ] Documentation and examples reflect the change.
- [ ] `mkdocs serve` works if docs were touched.
- [ ] No unrelated formatting changes were introduced.
- [ ] Each commit tells a coherent story (squash if needed).

Open the pull request against the `main` branch and provide context:

1. What problem does the change solve?
2. How does it work?
3. Any trade-offs or follow-up work the reviewers should be aware of?

Reviewers might request modifications – this is normal. Once everything looks good, the PR will be merged
and your work will become part of OpenDBT. Thank you for contributing! 🎉
