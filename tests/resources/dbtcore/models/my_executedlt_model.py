import dlt

@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "precision": 3}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123"},
           {"event_id": 2, "event_tstamp": "2025-02-30T10:00:00.321"}]


def model(dbt, connection: "Connection"):
    """

    :param dbt:
    :param connection: dbt connection to target database
    :return:
    """

    dbt.config(materialized="executepython")

    parts = [p.strip('"') for p in str(dbt.this).split('.')]
    database, schema, table_name = parts[0], parts[1], parts[2]
    pipeline_name = f"{database}_{schema}_{table_name}".replace(' ', '_')

    # IMPORTANT: here we are using dbt connection and mapping it to dlt destination
    # this might differ for each database
    dlt_destination = dlt.destinations.duckdb(connection.handle._env.conn)

    # IMPORTANT: here we are configuring and preparing dlt.pipeline for the model!
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt_destination,
        dataset_name=schema,
        dev_mode=False,
    )
    print("========================================================")
    print(f"INFO: DLT Pipeline pipeline_name:{pipeline.pipeline_name}")
    print(f"INFO: DLT Pipeline dataset_name:{pipeline.dataset_name}")
    print(f"INFO: DLT Pipeline dataset_name:{pipeline}")
    print(f"INFO: DLT Pipeline staging:{pipeline.staging}")
    print(f"INFO: DLT Pipeline destination:{pipeline.destination}")
    print(f"INFO: DLT Pipeline _pipeline_storage:{pipeline._pipeline_storage}")
    print(f"INFO: DLT Pipeline _schema_storage:{pipeline._schema_storage}")
    print(f"INFO: DLT Pipeline state:{pipeline.state}")
    print(f"INFO: DBT this:{dbt.this}")
    print("========================================================")
    load_info = pipeline.run(events(), dataset_name=schema, table_name=table_name)
    print(load_info)
    row_counts = pipeline.last_trace.last_normalize_info
    print(row_counts)
    print("========================================================")
    return None
