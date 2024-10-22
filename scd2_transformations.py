import json
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.types import ArrayType, MapType, StringType, StructType

from curated_user.config import Config, LatestEventIntervalType


def get_hash_columns(df: DataFrame, exclude_columns_list: list[str]) -> List[Column]:
    """
    Generates column list from Dataframe by excluding audit columns

    NOTE: The function WILL treat the ArrayType columns/fields/elements with different value orders
        as different values, since the different value orders of arrays represent different array values
        and in some cases the order of the values matters. So in order to detect the unchanged
        events, users need to sort the ArrayType columns/fields/elements in their SCD2 tables
        if they consider the order of the ArrayType does NOT matter in their use case.

    Args:
        df (dataframe): the dataframe for which hash needs to be derived
        exclude_columns_list (list): list of columns to exclude in has calculation
    Returns: List of columns required for generating hash
    """

    col_list = sorted(df.drop(*exclude_columns_list).columns)
    col_list_return = []

    # We have noticed that the `to_json` function will preserve the MapType key-value
    # pair orders, so we need to sort the MapType key-value pair orders to make sure that
    # the MapType values with different key-value pair orders will return the same JSON
    # string.
    # NOTE: the UDF `sort_json_udf` will NOT change/sort the ArrayType values
    sort_json_udf = F.udf(
        lambda x: json.dumps(json.loads(x), sort_keys=True) if x is not None else "",
        StringType(),
    )

    for column_name in col_list:
        data_type = df.select(column_name).schema[0].dataType
        if (
            isinstance(data_type, StructType)
            or isinstance(data_type, MapType)
            or isinstance(data_type, ArrayType)
        ):
            col_list_return.append(sort_json_udf(F.to_json(F.col(column_name))))
        else:
            col_list_return.append(F.col(column_name))

    return col_list_return


def get_hash(df: DataFrame, exclude_columns_list: list[str] | None) -> DataFrame:
    """
    Gereartes hash_md5 value based on column list generated from get_hash_columns,
    returns the Datagrame by adding the hash_md5 column

    Args:
        df (dataframe): the dataframe for which hash needs to be derived
        exclude_columns_list (list): list of columns to exclude in has calculation
    Returns: DataFrame
    """
    columns = get_hash_columns(df, exclude_columns_list if exclude_columns_list else [])
    return df.withColumn("hash_md5", F.md5(F.concat_ws("", *columns)))


def ignore_unchanged_data(
    df: DataFrame, target_table_df: DataFrame, config: Config
) -> DataFrame:
    """
    Compares the incremental changes from source with Target table, by using hash_md5 on non-audit columns,
    and ingores those unchanged updates for processing

    NOTE: The function WILL treat the ArrayType columns/fields/elements with different value orders
        as different values, since the different value orders of arrays represent different array values
        and in some cases the order of the values matters. So in order to detect the unchanged
        events, users need to sort the ArrayType columns/fields/elements in their SCD2 tables
        if they consider the order of the ArrayType does NOT matter in their use case.

    Args:
        df (dataframe): the dataframe with the incremental data to write to the table
        target_table_df (dataframe): the dataframe with the target table data
        config (Config): the job configuration object with relevant job parameters

    Returns: DataFrame
    """
    stg_df = get_hash(df, config.write.targetHashExcludeColumns)
    target_table_df = get_hash(target_table_df, config.write.targetHashExcludeColumns)

    new_inserts = stg_df.alias("updates").join(
        target_table_df.alias("target"),
        config.write.targetTableUniqueKey,
        how="left_anti",
    )

    changed_events = (
        stg_df.alias("updates")
        .join(target_table_df.alias("target"), config.write.targetTableUniqueKey)
        .where("updates.hash_md5 <> target.hash_md5 and target.is_current = true")
        .select("updates.*")
    )

    return changed_events.union(new_inserts).select("updates.*").drop("hash_md5")


def consolidate_with_target_df(
    source_df: DataFrame,
    target_df: DataFrame,
    taget_key_cols_list: list[str],
    is_full_table_reload: bool = False,
) -> DataFrame:
    """
    Compares the incremental changes from the source DataFrame with the target DataFrame's
    current records to filter the events that are newer than target.

    These filterd source records are union with target latest records.

    Args:
        source_df (DataFrame): The DataFrame containing the incremental data from the source.
        target_df (DataFrame): The DataFrame containing the data from the target table.
        target_key_cols_list (list): The list of key columns from the target table.
        is_full_table_reload(Boolean): Flag indicating if the current load type is full reload or incremental
    Returns:
        DataFrame: A DataFrame containing source incremetnal records & current target records
    """

    if not is_full_table_reload:
        curr_target_df = target_df.filter(F.col("is_current"))
        return (
            source_df.alias("incr")
            .join(curr_target_df.alias("tgt"), taget_key_cols_list, how="left")
            .where(
                "incr.valid_from_est_timestamp > coalesce(tgt.valid_from_est_timestamp,'1970-01-01 00:00:00.000') "
            )
            .select("incr.*")
            .unionByName(curr_target_df)
        )
    return source_df


def ignore_sequential_unchanged_events(
    df: DataFrame,
    partition_by: list[str],
    order_by: list[str],
    exclude_columns_list: list[str],
) -> DataFrame:
    """
    Compares all the records/events in the dataframe sequentially using hash_md5 on non-audit
    columns to identify changed updates & ingore all the unchanged events

    NOTE: The function WILL treat the ArrayType columns/fields/elements with different value orders
        as different values, since the different value orders of arrays represent different array values
        and in some cases the order of the values matters. So in order to detect the unchanged
        events, users need to sort the ArrayType columns/fields/elements in their SCD2 tables
        if they consider the order of the ArrayType does NOT matter in their use case.

    Args:
        df (DataFrame): The DataFrame containing evnet data
        partition_by (list): The list of key columns from source data
        order_by (list): The list of columns used for ordering the events from source
        exclude_columns_list (list): list of columns to exclude in has calculation
    Returns:
        DataFrame: A DataFrame by ignoring the unchanged events
    """
    hash_md5_col, unchanged_col = "hash_md5", "_unchanged"
    flag_unchanged_events = F.when(
        F.lag(hash_md5_col).over(
            Window.partitionBy(partition_by).orderBy(*[F.asc(c) for c in order_by])
        )
        == F.col(hash_md5_col),
        True,
    ).otherwise(False)
    return (
        df.transform(get_hash, exclude_columns_list)
        .withColumn(unchanged_col, flag_unchanged_events)
        .where(~F.col(unchanged_col))
        .drop(hash_md5_col, unchanged_col)
    )


def build_scd2_new_events(
    seq_changed_event_df: DataFrame, partition_by: list[str], order_by: list[str]
) -> DataFrame:
    """
    Reads input dataframe, derives valid_to_est_timestamp based on next record's
    valid_from_est_timestamp

    The latest record for each key column combination will be marked with
    valid_to_est_timestamp = "9999-12-31 00:00:00.000" and is_current=True.

    Args:
        df (DataFrame): The DataFrame containing evnet data
        partition_by (list): The list of key columns from source data
        order_by (list): The list of columns used for ordering the events from source
    Returns:
        DataFrame: A DataFrame with derived SCD2 attributes valid_from_est_timestamp,
        valid_to_est_timestamp and is_current
    """
    upper_boundary_ts = F.to_timestamp(
        F.lit("9999-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss"
    )
    return seq_changed_event_df.withColumn(
        "valid_to_est_timestamp",
        F.coalesce(
            F.lag("valid_from_est_timestamp").over(
                Window.partitionBy(partition_by).orderBy(*[F.desc(c) for c in order_by])
            ),
            upper_boundary_ts,
        ),
    ).withColumn(
        "is_current",
        F.when(F.col("valid_to_est_timestamp") == upper_boundary_ts, True).otherwise(
            False
        ),
    )


def process_deleted_events(df: DataFrame) -> DataFrame:
    """
    set is_current and valid_to_est_timestamp based on etl_metadata_struct.is_deleted

    Args:
        df (DataFrame): The DataFrame containing evnet data
    Returns:
        DataFrame: A DataFrame with updated SCD2 attributes valid_to_est_timestamp and is_current
    """
    return df.withColumn(
        "is_current",
        F.when(
            F.col("etl_metadata_struct.is_deleted"),
            False,
        ).otherwise(F.col("is_current")),
    ).withColumn(
        "valid_to_est_timestamp",
        F.when(
            F.col("etl_metadata_struct.is_deleted"),
            F.col("valid_from_est_timestamp"),
        ).otherwise(F.col("valid_to_est_timestamp")),
    )


def get_latest_events_for_each_interval(df: DataFrame, config: Config) -> DataFrame:
    """
    Gets a dataframe that has the latest events partiioned by the IntervalType (DAY/HOUR/EVENT)
    & ordered by the list of columns defined in the config file

    Defalt IntervalType is "DAY" and default ordering is by "envelopeTime"

    Args:
        df (dataframe): the dataframe representing the data that needs to be processed
        config (Config): the job configuration object with relevant job parameters
    Returns:
        dataframe: A dataframe containing latest events by interval type.
    """
    interval_col, rank_col = "_interval", "rn"
    if config.read.latestEventIntervalType == LatestEventIntervalType.DAY:
        df = df.withColumn(
            interval_col,
            F.to_date(
                F.from_utc_timestamp(str(config.read.eventTimestampCol), "UTC-5"),
            ),
        )
    elif config.read.latestEventIntervalType == LatestEventIntervalType.HOUR:
        df = df.withColumn(
            interval_col,
            F.date_trunc(
                "hour",
                F.from_utc_timestamp(str(config.read.eventTimestampCol), "UTC-5"),
            ),
        )
    elif config.read.latestEventIntervalType == LatestEventIntervalType.EVENT:
        df = df.withColumn(
            interval_col,
            F.from_utc_timestamp(str(config.read.eventTimestampCol), "UTC-5"),
        )

    return (
        df.withColumn(
            rank_col,
            F.row_number().over(
                Window.partitionBy(
                    config.read.sourcePartitionKey + [interval_col]
                ).orderBy(*[F.desc(c) for c in config.read.orderBy])
            ),
        )
        .where(F.col(rank_col) == 1)
        .drop(rank_col, interval_col)
    )
