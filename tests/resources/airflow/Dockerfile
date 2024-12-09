FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

# install additional packages
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow pyproject.toml /tmp/opendbt/pyproject.toml

RUN pip install -e /tmp/opendbt/.[test]
EXPOSE 8080
