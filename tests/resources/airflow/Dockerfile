FROM apache/airflow:slim-2.5.2-python3.8
LABEL authors="opendbt"

# install additional packages
COPY --chown=airflow:airflow opendbt /tmp/opendbt/opendbt
COPY --chown=airflow:airflow README.md /tmp/opendbt/README.md
COPY --chown=airflow:airflow setup.py /tmp/opendbt/setup.py
COPY --chown=airflow:airflow tests/resources/dbttest /opt/dbttest
COPY --chown=airflow:airflow opendbt/macros /opt/dbttest/macros

RUN pip install -e /tmp/opendbt/
EXPOSE 8080
