FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

USER airflow

# install additional packages
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow pyproject.toml /tmp/opendbt/pyproject.toml

RUN pip install --upgrade pip==25.0.1 \
    && pip install "dbt-core==1.8.*" \
    "duckdb>=1.0.0,<1.4.0" \
    "dbt-duckdb==1.8.*" \
    --prefer-binary --no-cache-dir

RUN pip install -e /tmp/opendbt

