import typing
from datetime import datetime
from functools import reduce
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window, WindowSpec
from pyspark.sql.types import BooleanType, TimestampType

from curated_user.config import Config


def add_audit_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Adds create and update timstamp columns derived from current timestamp.
    Args:
        df (DataFrame): Dataframe
    Returns:
        DataFrame: Structured Streaming DataFrame with the
        "etl_create_est_timestamp","etl_update_est_timestamp"
    """
    current_est_timestamp = F.from_utc_timestamp(F.current_timestamp(), "UTC-5")
    return df.withColumn(
        "etl_metadata_struct",
        F.struct(
            current_est_timestamp.alias("etl_create_est_timestamp"),
            current_est_timestamp.alias("etl_update_est_timestamp"),
        ),
    )


def get_latest_batch_data(
    src_df: DataFrame,
    previous_process_time: datetime,
) -> DataFrame:
    """
    Get the latest batch data from the source DataFrame since the previous batch process.
    Args:
        src_df (DataFrame): The Spark DataFrame
        previous_process_time (datetime): The process time of the previous batch process
    Returns:
        DataFrame: The latest batch data from the source DataFrame
    """
    if previous_process_time:
        batch_start_date = previous_process_time.date()
        batch_start_hour = str(previous_process_time.hour).zfill(2)
        src_df = src_df.where(
            (F.col("date") > batch_start_date)
            | (
                (F.col("date") == batch_start_date)
                & (F.col("hour") >= batch_start_hour)
            )
        )
    return src_df


def get_src_processed_time(list_of_dfs: list[DataFrame]) -> datetime:
    """
    Get the min value of the max processed/ETL timestamps from different source DataFrames.
    Args:
        list_of_dfs (list[DataFrame]): a list of the max processed/ETL timestamps DataFrames
    Returns:
        datetime: the min value datetime of the max processed/ETL timestamps
    """
    src_processed_time = (
        reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            [
                df.select(
                    F.max(F.concat(F.col("date"), F.lit("-"), F.col("hour"))).alias(
                        "maxDateHour"
                    )
                )
                for df in list_of_dfs
            ],
        )
        .select(F.to_timestamp(F.min(F.col("maxDateHour")), "yyyy-MM-dd-HH"))
        .collect()[0][0]
    )
    return typing.cast(datetime, src_processed_time)


def union_crud_events_df(list_of_dfs: List[DataFrame]) -> DataFrame:
    """
    Unions a list of dataframes by column name
    Args:
        list_of_dfs (List[DataFrame]): a list of dataframes representing the raw input data
    Returns:
        Dataframe: a dataframe with the unioned results of the input dataframes
    """
    return reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), list_of_dfs)


def get_latest_events_in_batch(
    batch_output_df: DataFrame,
    window_spec: Window | WindowSpec,
    config: Config,
) -> DataFrame:
    """
    Gets a dataframe that has the latest event in the batch according to ordering rules
    Args:
        batch_output_df (dataframe): the dataframe representing the batch that needs to be processed
        window_spec (Window): the window specification
        create_entity_patter (str): pattern to look for to indicate this is a create event
        config (Config): the job configuration object with relevant job parameters
    Returns:
        dataframe: the input dataframe transformed in such a way that only the latest events in the dataframe
            are kept
    """
    return (
        batch_output_df.withColumn(
            "rn",
            F.row_number().over(
                window_spec.orderBy(*[F.desc(c) for c in config.read.orderBy])
            ),
        )
        .where(F.col("rn") == 1)
        .drop("rn")
    )


def add_source_timestamp_struct(
    df: DataFrame,
    create_timestamp_col: str | Column,
    update_timestamp_col: str | Column,
) -> DataFrame:
    """
    Add the source_timestamp_struct column based on the create timestamp and
    the update timestamp columns/fields which will be converted to the EST timestamps.
    Args:
        df (DataFrame): Spark DataFrame
        create_timestamp_col (str): event create timestamp column name in UTC time zone
        update_timestamp_col (str): event update timestamp column name in UTC time zone
    Returns:
        DataFrame: DataFrame with the source_timestamp_struct column
    """
    return df.withColumn(
        "source_timestamp_struct",
        F.struct(
            F.from_utc_timestamp(create_timestamp_col, "UTC-5").alias(
                "create_est_timestamp"
            ),
            F.from_utc_timestamp(update_timestamp_col, "UTC-5").alias(
                "update_est_timestamp"
            ),
        ),
    )


def add_scd2_audit_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Adds SCD type2 audit columns to the DataFramre
    Args:
        df (DataFrame): DataFrame
    Returns:
        DataFrame: DataFrame with "valid_from_est_timestamp", "valid_to_est_timestamp" and "is_current" columns
    """

    return (
        df.withColumn("valid_from_est_timestamp", F.lit(None).cast(TimestampType()))
        .withColumn("valid_to_est_timestamp", F.lit(None).cast(TimestampType()))
        .withColumn("is_current", F.lit(None).cast(BooleanType()))
    )


def get_events_until_date(df: DataFrame, run_date: str) -> DataFrame:
    """
    Filter the input DataFrame to select all events with the envelopeTime falling
    within the specified date range in the EST timezone, up to, but not including,
    the run_date parameter.

    Args:
        df (DataFrame): The input DataFrame containing events data.
        run_date (str): The end date of the date range in 'YYYY-MM-DD' format.

    Returns:
        DataFrame: A filtered DataFrame containing events up to, but not including,
        the specified run_date.
    """
    return df.where(
        F.from_utc_timestamp("envelopeTime", "GMT-5").cast("date")
        < datetime.strptime(run_date, "%Y-%m-%d").date()
    )


def add_audit_columns_v2(
    df: DataFrame,
    delete_entity_pattern: str,
    event_type_column: str = "envelopeType",
) -> DataFrame:
    """
    adds etl_metadata_struct
            (1) etl_create_est_timestamp
            (2) etl_update_est_timestamp
            (3) is_deleted
    args:
        df (DataFrame): Dataframe
        delete_entity_pattern (str): search pattern to find entity deletion events, simple string contains matching
        event_type_column (str): indicates name of column in the df that indicates the type of event.
                                 Default value assigned is "envelopeType"
    returns:
        DataFrame: structured with derived etl_metadata_struct
    """
    current_est_timestamp = F.from_utc_timestamp(F.current_timestamp(), "UTC-5")
    return df.withColumn(
        "etl_metadata_struct",
        F.struct(
            current_est_timestamp.alias("etl_create_est_timestamp"),
            current_est_timestamp.alias("etl_update_est_timestamp"),
            F.when(F.col(event_type_column).contains(delete_entity_pattern), True)
            .otherwise(False)
            .alias("is_deleted"),
        ),
    )


def add_is_removed_column(
    df: DataFrame, delete_entity_pattern: str, event_type_column: str = "eventType"
) -> DataFrame:
    """
    Adds is_removed column to the DataFrame using the event type column from input.

    Args:
        df (DataFrame): DataFrame
        delete_entity_pattern (str): Search pattern to find entity deletion events.
        event_type_column (str): Indicates the name of the column in the DataFrame that indicates the type of event.

    Returns:
        DataFrame: DataFrame with "is_removed" column
    """
    return df.withColumn(
        "is_removed",
        F.when(
            F.col(event_type_column).contains(delete_entity_pattern), True
        ).otherwise(False),
    )


def get_events_from_day(df: DataFrame, run_date: str) -> DataFrame:
    """
    Return the input DataFrame after applying a filter to select all events with the
    envelopeTime with the specified date in EST timezone.

    Args:
        df (DataFrame): Input events DataFrame
        run_date (str): The date for which filter applies in 'YYYY-MM-DD' format

    Returns:
        DataFrame: Input DataFrame with date filter applied
    """
    return df.where(
        F.from_utc_timestamp("envelopeTime", "GMT-5").cast("date")
        == datetime.strptime(run_date, "%Y-%m-%d").date()
    )


def add_request_context_struct(
    df: DataFrame,
) -> DataFrame:
    """
    Adds request_context_struct column to the DataFrame
    Args:
        df (DataFrame): DataFrame
    Returns:
        DataFrame: DataFrame with "request_context_struct" column
    """
    return df.withColumn(
        "request_context_struct",
        F.struct(
            F.col("requestContext.deviceId").alias("device_id"),
            F.col("requestContext.sessionId").alias("session_id"),
            F.col("requestContext.tokenId").alias("token_id"),
            F.col("requestContext.actionId").alias("action_id"),
            F.col("requestContext.identityId").alias("identity_id"),
            F.col("requestContext.accountId").alias("account_id"),
            F.col("requestContext.profileId").alias("profile_id"),
            F.col("requestContext.agentId").alias("agent_id"),
            F.col("requestContext.logicalClock").alias("logical_clock"),
        ),
    ).drop(F.col("requestContext"))
