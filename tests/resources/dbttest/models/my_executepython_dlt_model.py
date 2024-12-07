import dlt


@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "precision": 3}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123"},
           {"event_id": 2, "event_tstamp": "2025-02-30T10:00:00.321"}]


def model(dbt, session):
    dbt.config(materialized="executepython")
    print("========================================================")
    print(f"INFO: DLT Version:{dlt.version.__version__}")
    print(f"INFO: DBT Duckdb Session:{type(session)}")
    print(f"INFO: DBT Duckdb Connection:{type(session._env.conn)}")
    print("========================================================")
    p = dlt.pipeline(
        pipeline_name="dbt_dlt",
        destination=dlt.destinations.duckdb(session._env.conn),
        dataset_name=dbt.this.schema,
        dev_mode=False,
    )
    load_info = p.run(events())
    print(load_info)
    row_counts = p.last_trace.last_normalize_info
    print(row_counts)
    print("========================================================")
    return None
