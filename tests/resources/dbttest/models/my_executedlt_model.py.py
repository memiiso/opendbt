import dlt
from dlt.pipeline import TPipeline


@dlt.resource(
    columns={"event_tstamp": {"data_type": "timestamp", "precision": 3}},
    primary_key="event_id",
)
def events():
    yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123"},
           {"event_id": 2, "event_tstamp": "2025-02-30T10:00:00.321"}]


def model(dbt, pipeline: TPipeline):
    """

    :param dbt:
    :param pipeline: Pre-configured dlt pipeline. dlt target connection and dataset is pre-set using the model config!
    :return:
    """
    dbt.config(materialized="executedlt")
    print("========================================================")
    print(f"INFO: DLT Pipeline pipeline_name:{pipeline.pipeline_name}")
    print(f"INFO: DLT Pipeline dataset_name:{pipeline.dataset_name}")
    print(f"INFO: DLT Pipeline staging:{pipeline.staging}")
    print(f"INFO: DLT Pipeline destination:{pipeline.destination}")
    print(f"INFO: DLT Pipeline _pipeline_storage:{pipeline._pipeline_storage}")
    print(f"INFO: DLT Pipeline _schema_storage:{pipeline._schema_storage}")
    print(f"INFO: DLT Pipeline state:{pipeline.state}")
    print("========================================================")
    load_info = pipeline.run(events())
    print(load_info)
    row_counts = pipeline.last_trace.last_normalize_info
    print(row_counts)
    print("========================================================")
    return None
