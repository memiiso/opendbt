FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

USER airflow

RUN pip install "dbt-core==1.8.7" \
    "duckdb==1.2.2" \
    "dbt-duckdb==1.8.4" \
    --prefer-binary


# install additional packages
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow pyproject.toml /tmp/opendbt/pyproject.toml
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt

RUN pip install -e /tmp/opendbt

