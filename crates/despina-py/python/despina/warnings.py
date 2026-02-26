class DespinaWarning(UserWarning):
    """Non-fatal data quality warning raised by high-level workflows.

    This warning is typically emitted for recoverable ingestion issues such as
    missing OD pairs when ``on_missing_od="warn"``.
    """
