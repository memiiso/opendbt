FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

# install additional packages
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow pyproject.toml /tmp/opendbt/pyproject.toml

RUN pip install dbt-core==1.8.*
RUN pip install dbt-duckdb==1.8.*
RUN pip install opendbt
EXPOSE 8080
