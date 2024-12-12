def email_model_warnings_callback(event: "EventMsg"):
    # event.info.name=LogTestResult event.info.level=warn
    if event.info.name == "LogTestResult" and event.info.level == "warn":
        print(f"EMAIL WARNING {event.data} {event.info}")


def email_test_errors_callback(event: "EventMsg"):
    # event.info.name=LogTestResult event.info.level=error
    if event.info.name == "LogTestResult" and event.info.level == "error":
        print(f"EMAIL ERROR {event.data} {event.info}")
        # @TODO use env var airflow AIRFLOW__DBT__EMAIL_ALERT_TO
