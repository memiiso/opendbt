FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

# Install build dependencies for DuckDB compilation
# DuckDB requires g++, make, and Python headers to build from source
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    gcc \
    make \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# install additional packages
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow pyproject.toml /tmp/opendbt/pyproject.toml

RUN pip install --upgrade pip
RUN pip install "dbt-core==1.8.*"

# Install duckdb with workaround for source dist build issue in 1.3.0+
# See: https://github.com/duckdb/duckdb/issues/14367
RUN OVERRIDE_GIT_DESCRIBE="v1.3.0" pip install "duckdb>=1.0.0,<1.4.0" --no-cache-dir

RUN pip install "dbt-duckdb==1.8.*" opendbt

EXPOSE 8080
