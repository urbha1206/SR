from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


class DomainEventReader:
    """
    The DomainEventReader class contains the transformation functions to read
    the source domain events.
    """

    @staticmethod
    def get_events_by_date(
        df: DataFrame,
        start_date: str | None = None,
        end_date: str | None = None,
        event_ts_col: str = "envelopeTime",
        tz: str = "UTC-5",
    ) -> DataFrame:
        """
        Gets the events DataFrame after applying a filter to select all events with the event timestamp
        column (`envelopeTime` by default) that satisfied the specified date range in the specified timezone
        (EST by default).
        If `start_date` and `end_date` are both specified, the output DataFrame will contain the events data
        between the `start_date` and `end_date` inclusively.
        If `start_date` is specified and `end_date` is NOT specified, the output DataFrame will only contain
        the events data within the `start_date`.
        If `start_date` is NOT specified and `end_date` is specified, the output DataFrame will contain
        all the events data before the `end_date` inclusively.
        - Args:
            - df (DataFrame): The input events DataFrame with the specified `event_ts_col` column
            - start_date (str): The start date for which filter applies in 'YYYY-MM-DD' format.
            - end_date (str): The end date for which filter applies in 'YYYY-MM-DD' format.
            - event_ts_col (str): The event timestamp column for the events in the DataFrame
            - tz (str): The timezone of the `start_date` and `end_date`
        - Returns:
            - DataFrame: The events DataFrame with the specified date filter applied
        """
        if start_date is not None and end_date is not None:
            return df.where(
                (
                    F.from_utc_timestamp(event_ts_col, tz).cast("date")
                    >= datetime.strptime(start_date, "%Y-%m-%d").date()
                )
                & (
                    F.from_utc_timestamp(event_ts_col, tz).cast("date")
                    <= datetime.strptime(end_date, "%Y-%m-%d").date()
                )
            )
        elif start_date is not None and end_date is None:
            return df.where(
                F.from_utc_timestamp(event_ts_col, tz).cast("date")
                == datetime.strptime(start_date, "%Y-%m-%d").date()
            )
        elif start_date is None and end_date is not None:
            return df.where(
                F.from_utc_timestamp(event_ts_col, tz).cast("date")
                <= datetime.strptime(end_date, "%Y-%m-%d").date()
            )
        else:
            raise Exception(
                """
                At least one of the 'start_date' or 'end_date' input parameters needs to
                be specified for 'get_events_by_date' function.
                """
            )