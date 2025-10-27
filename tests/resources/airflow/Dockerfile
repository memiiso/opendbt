FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

USER airflow

# install additional packages
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow pyproject.toml /tmp/opendbt/pyproject.toml

RUN pip install --upgrade pip
RUN pip install "dbt-core==1.8.*"
RUN pip install "duckdb>=1.0.0,<1.4.0" --prefer-binary --no-cache-dir

# Install dbt-duckdb and local opendbt code (not PyPI version)
RUN pip install "dbt-duckdb==1.8.*"
RUN pip install -e /tmp/opendbt
