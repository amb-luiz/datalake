"""Optional downstream loading contract; local ETL has no database dependency."""

from typing import Protocol


class TransactionSink(Protocol):
    def load(self, report_path: str) -> None:
        """Load committed Parquet paths from data_quality.json, idempotently by generation.

        Implement in a separate consumer/DAG for the selected destination. No loader
        is invoked by the extraction DAG, so its absence cannot block local output.
        """
        ...
