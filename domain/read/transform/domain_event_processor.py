import json
from datetime import datetime
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)

from data_eng_etl_lib.transform.latest_event_interval_type import (
    LatestEventIntervalType,
)


class DomainEventProcessor:
    """
    The DomainEventProcessor class contains the domain event transformation functions.
    """

    @staticmethod
    def add_source_timestamp_struct(
        df: DataFrame,
        create_timestamp_col: str | Column,
        update_timestamp_col: str | Column,
    ) -> DataFrame:
        """
        Add the source_timestamp_struct column based on the create timestamp and
        the update timestamp columns/fields which will be converted to the EST timestamps.
        - Args:
            - df (DataFrame): Spark DataFrame
            - create_timestamp_col (str): event create timestamp column name in UTC time zone
            - update_timestamp_col (str): event update timestamp column name in UTC time zone
        - Returns:
            - DataFrame: DataFrame with the source_timestamp_struct column
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

    @staticmethod
    def union_dfs(
        dfs: list[DataFrame], allow_missing_columns: bool = True
    ) -> DataFrame:
        """
        Unions a list of dataframes by column name, and allow missing columns by default.
        - Args:
            - dfs (List[DataFrame]): a list of dataframes representing the raw input data
        - Returns:
            - Dataframe: a dataframe with the unioned results of the input dataframes
        """
        return reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=allow_missing_columns),
            dfs,
        )

    @staticmethod
    def get_new_events_since_previous_processed(
        src_df: DataFrame,
        previous_processed_time: datetime,
    ) -> DataFrame:
        """
        Get the new events from the source DataFrame since the previous process.

        NOTE: The results will include the last hour source data from the previous processed data.

        - Args:
            - src_df (DataFrame): The source Spark DataFrame
            - previous_processed_time (datetime): The max date-hour timestamp of the previous processed data
        - Returns:
            - DataFrame: The new data from the source DataFrame since the previous process
        """
        if previous_processed_time:
            batch_start_date = previous_processed_time.date()
            batch_start_hour = str(previous_processed_time.hour).zfill(2)
            src_df = src_df.where(
                (F.col("date") > batch_start_date)
                | (
                    (F.col("date") == batch_start_date)
                    & (F.col("hour") >= batch_start_hour)
                )
            )
        return src_df

    @staticmethod
    def add_audit_columns(
        df: DataFrame,
    ) -> DataFrame:
        """
        Adds `etl_metadata_struct` struct with EST create and update timestamp fields
        derived from the current timestamp.
        - etl_metadata_struct
            - (1) etl_create_est_timestamp
            - (2) etl_update_est_timestamp
        - Args:
            - df (DataFrame): A Spark Dataframe
        - Returns:
            - DataFrame: a structured DataFrame structured with derived etl_metadata_struct
        """
        current_est_timestamp = F.from_utc_timestamp(F.current_timestamp(), "UTC-5")
        return df.withColumn(
            "etl_metadata_struct",
            F.struct(
                current_est_timestamp.alias("etl_create_est_timestamp"),
                current_est_timestamp.alias("etl_update_est_timestamp"),
            ),
        )

    @staticmethod
    def add_audit_columns_v2(
        df: DataFrame,
        delete_entity_pattern: str,
        event_type_column: str = "envelopeType",
    ) -> DataFrame:
        """
        Adds the `etl_metadata_struct` column with the required fields:
        - etl_metadata_struct
            - (1) etl_create_est_timestamp
            - (2) etl_update_est_timestamp
            - (3) is_deleted
        - Args:
            - df (DataFrame): Dataframe
            - delete_entity_pattern (str): Pattern string to find entity deletion events (contains matching string)
            - event_type_column (str): The name of column in the df that indicates the type of event. Default value
            assigned is "envelopeType"
        - Returns:
            - DataFrame: A structured DataFrame with the derived `etl_metadata_struct` column
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

    @staticmethod
    def get_top_rank_events(
        df: DataFrame,
        partition_by: list[str | Column],
        order_by: list[Column],
    ) -> DataFrame:
        """
        Gets a DataFrame that has the top rank events according to ordering rules from the inputs.

        NOTE: Users should make sure the `order_by` input parameter will give the determined ordering, otherwise
            it might cause exceptions in Spark actions or return undetermined results.

        - Args:
            - df (DataFrame): The DataFrame contains `partition_by` and `order_by` columns
            - partition_by (list[str | Column]): The parition columns used by window specification
            - order_by (list[Column]): The Spark columns with specified orders (e.g. `[F.desc(c) for c in col_names]`)
        - Returns:
            - DataFrame: The Dataframe with the top rank events regarding to the specified ordering rule
        """
        return (
            df.withColumn(
                "rn",
                F.row_number().over(
                    Window.partitionBy(partition_by).orderBy(*order_by)
                ),
            )
            .where(F.col("rn") == 1)
            .drop("rn")
        )

    @staticmethod
    def get_latest_events_for_each_interval(
        df: DataFrame,
        event_ts_col: str,
        partition_by: list[str],
        order_by: list[str],
        interval_type: LatestEventIntervalType,
    ) -> DataFrame:
        """
        Gets a DataFrame that has the latest events for each specified interval (DAY/HOUR/EVENT), and the
        latest (top rank) events are based on the parition_by columns (combined with the specified interval)
        and the order_by columns with DESCENDING orders.
        - Args:
            - df (DataFrame): The events DataFrame with the specified `event_ts_col`, `partition_by` and
              `order_by` columns
            - event_ts_col (str): The event timestamp column for the events in the DataFrame
            - partition_by (list[str]): The parition columns which is used to parition the events when doing ordering
            - order_by (list[str]): The odering columns which is used to order the events
        - Returns:
            - DataFrame: A DataFrame containing latest events for each interval.
        """
        interval_col, rank_col = "_interval", "rn"
        if interval_type == LatestEventIntervalType.DAY:
            df = df.withColumn(
                interval_col,
                F.to_date(
                    F.from_utc_timestamp(event_ts_col, "UTC-5"),
                ),
            )
        elif interval_type == LatestEventIntervalType.HOUR:
            df = df.withColumn(
                interval_col,
                F.date_trunc(
                    "hour",
                    F.from_utc_timestamp(event_ts_col, "UTC-5"),
                ),
            )
        elif interval_type == LatestEventIntervalType.EVENT:
            df = df.withColumn(
                interval_col,
                F.from_utc_timestamp(event_ts_col, "UTC-5"),
            )
        return (
            df.withColumn(
                rank_col,
                F.row_number().over(
                    Window.partitionBy(partition_by + [interval_col]).orderBy(
                        *[F.desc(c) for c in order_by]
                    )
                ),
            )
            .where(F.col(rank_col) == 1)
            .drop(rank_col, interval_col)
        )

    @staticmethod
    def add_scd2_audit_columns(
        df: DataFrame, event_ts_col: str = "envelopeTime"
    ) -> DataFrame:
        """
        Adds SCD type2 audit columns `valid_from_est_timestamp`, `valid_to_est_timestamp` and
        `is_current` to the DataFrame.
        - Args:
            - df (DataFrame): The events DataFrame
        - Returns:
            - DataFrame: DataFrame with `valid_from_est_timestamp`, `valid_to_est_timestamp` and `is_current` columns
        """
        return (
            df.withColumn(
                "valid_from_est_timestamp", F.from_utc_timestamp(event_ts_col, "UTC-5")
            )
            .withColumn("valid_to_est_timestamp", F.lit(None).cast(TimestampType()))
            .withColumn("is_current", F.lit(None).cast(BooleanType()))
        )

    @staticmethod
    def consolidate_with_scd2_target_df(
        source_df: DataFrame,
        target_df: DataFrame,
        taget_key_cols_list: list[str],
        is_full_table_reload: bool = False,
    ) -> DataFrame:
        """
        Compares the incremental changes from the source DataFrame with the target DataFrame's
        current records to filter the events that are newer than the target events.
        And then union these filtered source records with the target current records.

        NOTE: The source DataFrame and the target DataFrame should have the same schema.

        - Args:
            - source_df (DataFrame): The DataFrame with the incremental events data from the source.
            - target_df (DataFrame): The DataFrame with the events data from the target table.
            - target_key_cols_list (list): The list of key columns from the target table for joining.
            - is_full_table_reload(Boolean): Flag indicating if the current load type is full reload or incremental
        - Returns:
            - DataFrame: A DataFrame containing source incremetnal records & current target records
        """
        if not is_full_table_reload:
            curr_target_df = target_df.filter(F.col("is_current"))
            return (
                source_df.alias("incr")
                .join(curr_target_df.alias("tgt"), taget_key_cols_list, how="left")
                .where(
                    "incr.valid_from_est_timestamp > coalesce(tgt.valid_from_est_timestamp,'1970-01-01 00:00:00.000')"
                )
                .select("incr.*")
                .unionByName(curr_target_df)
            )
        return source_df

    @staticmethod
    def get_hash_columns(
        df: DataFrame, exclude_columns_list: list[str]
    ) -> list[Column]:
        """
        Generates a column list from the Dataframe by excluding the audit/specified columns.

        NOTE: The function WILL treat the ArrayType columns/fields/elements with different value orders
            as different values, since the different value orders of arrays represent different array values
            and in some cases the order of the values matters. So in order to detect the unchanged
            events, users need to sort the ArrayType columns/fields/elements in their SCD2 tables
            if they consider the order of the ArrayType does NOT matter in their use case.

        NOTE: The `exclude_columns_list` feature does NOT support for nested fields for now, and
            we will need to support for excluding nested fields.

        - Args:
            - df (DataFrame): The DataFrame for which hash values need to be derived
            - exclude_columns_list (list): A list of columns which will be excluded in the hashing calculation
        - Returns:
            - list[Columns]: List of columns required for generating hash values
        """

        col_list = sorted(df.drop(*exclude_columns_list).columns)
        col_list_return = []

        # We have noticed that the `to_json` function will preserve the MapType key-value
        # pair orders, so we need to sort the MapType key-value pair orders to make sure that
        # the MapType values with different key-value pair orders will return the same JSON
        # string.
        # NOTE: the UDF `sort_json_udf` will NOT change/sort the ArrayType values
        sort_json_udf = F.udf(
            lambda x: json.dumps(json.loads(x), sort_keys=True)
            if x is not None
            else "",
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

    @staticmethod
    def get_md5_hash(
        df: DataFrame, exclude_columns_list: list[str] | None, md5_hash_col: str
    ) -> DataFrame:
        """
        Generates md5 hash values based on all the columns from the DataFrame except the columns in
        the `exclude_columns_list`, and then returns the DataFrame with the md5 hash column.

        NOTE: The function WILL treat the ArrayType columns/fields/elements with different value orders
            as different values, since the different value orders of arrays represent different array values
            and in some cases the order of the values matters. So in order to detect the unchanged
            events, users need to sort the ArrayType columns/fields/elements in their SCD2 tables
            if they consider the order of the ArrayType does NOT matter in their use case.

        NOTE: The `exclude_columns_list` feature does NOT support for nested fields for now, and
            we will need to support for excluding nested fields.

        - Args:
            - df (dataframe): The DataFrame for which md5 hash values needs to be derived
            - exclude_columns_list (list): A list of columns to be excluded in the md5 hashing calculation
            - md5_hash_col (str): The name of the md5 hash column which will be added to the DataFrame
        - Returns:
            - DataFrame: The DataFrame with the md5 hash column
        """
        columns = DomainEventProcessor.get_hash_columns(
            df, exclude_columns_list if exclude_columns_list else []
        )
        return df.withColumn(md5_hash_col, F.md5(F.concat_ws("", *columns)))

    @staticmethod
    def ignore_sequential_unchanged_events(
        df: DataFrame,
        partition_by: list[str],
        order_by: list[str],
        exclude_columns_list: list[str],
    ) -> DataFrame:
        """
        Compares all the records/events in the dataframe sequentially using md5 hash values on non-audit
        columns to identify changed updates and ingore all the unchanged events.

        NOTE: The function WILL treat the ArrayType columns/fields/elements with different value orders
            as different values, since the different value orders of arrays represent different array values
            and in some cases the order of the values matters. So in order to detect the unchanged
            events, users need to sort the ArrayType columns/fields/elements in their SCD2 tables
            if they consider the order of the ArrayType does NOT matter in their use case.

        NOTE: The `exclude_columns_list` feature does NOT support for nested fields for now, and
            we will need to support for excluding nested fields.

        - Args:
            - df (DataFrame): The DataFrame with the specified `partition_by` and `order_by` columns
            - partition_by (list[str]): The parition columns which is used to parition the events when doing ordering
            - order_by (list[str]): The odering columns which is used to order the events
            - exclude_columns_list (list[str]): A list of columns to be excluded in the md5 hashing calculation
        - Returns:
            - DataFrame: A DataFrame with the sequential changed events
        """
        md5_hash_col, unchanged_col = "md5_hash", "_unchanged"
        flag_unchanged_events = F.when(
            F.lag(md5_hash_col).over(
                Window.partitionBy(partition_by).orderBy(*[F.asc(c) for c in order_by])
            )
            == F.col(md5_hash_col),
            True,
        ).otherwise(False)
        return (
            df.transform(
                DomainEventProcessor.get_md5_hash, exclude_columns_list, md5_hash_col
            )
            .withColumn(unchanged_col, flag_unchanged_events)
            .where(~F.col(unchanged_col))
            .drop(md5_hash_col, unchanged_col)
        )

    @staticmethod
    def build_scd2_events(
        seq_changed_event_df: DataFrame, partition_by: list[str], order_by: list[str]
    ) -> DataFrame:
        """
        Builds the SCD2 events Dataframe by deriving the `valid_to_est_timestamp` column based
        on the next record's `valid_from_est_timestamp` column.
        And the latest/current record for each key column combination will be marked with
        `valid_to_est_timestamp="9999-12-31 00:00:00.000"` and `is_current=True`.
        - Args:
            - df (DataFrame): The sequentially changed events DataFrame
            - partition_by (list[str]): The parition columns which is used to parition the events when doing ordering
            - order_by (list[str]): The odering columns which is used to order the events
        - Returns:
            - DataFrame: A DataFrame with the derived SCD2 columns `valid_from_est_timestamp`,
                         `valid_to_est_timestamp` and `is_current`
        """
        upper_boundary_ts = F.to_timestamp(
            F.lit("9999-12-31 00:00:00"), "yyyy-MM-dd HH:mm:ss"
        )
        return seq_changed_event_df.withColumn(
            "valid_to_est_timestamp",
            F.coalesce(
                F.lag("valid_from_est_timestamp").over(
                    Window.partitionBy(partition_by).orderBy(
                        *[F.desc(c) for c in order_by]
                    )
                ),
                upper_boundary_ts,
            ),
        ).withColumn(
            "is_current",
            F.when(
                F.col("valid_to_est_timestamp") == upper_boundary_ts, True
            ).otherwise(False),
        )

    @staticmethod
    def process_deleted_events(df: DataFrame) -> DataFrame:
        """
        Sets `is_current` and `valid_to_est_timestamp` based on `etl_metadata_struct.is_deleted`
        - Args:
            - df (DataFrame): The DataFrame with the `etl_metadata_struct.is_deleted` field
        - Returns:
            - DataFrame: A DataFrame with updated SCD2 columns `valid_to_est_timestamp` and `is_current`
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

    @staticmethod
    def add_request_context_struct(
        df: DataFrame, col_name: str = "request_context_struct"
    ) -> DataFrame:
        """
        Adds a structured column of request context to the DataFrame.
        This column is composed of nested fields from the "requestContext" structure,
        and the `requestContext` structure definition is from the corresponding SXM services events.
        - Args:
            - df (DataFrame): A DataFrame with the `requestContext` struct column
            - col_name (str): The column name of the request context struct. Default value
                assigned is "request_context_struct"
        - Returns:
            - DataFrame: A DataFrame with the request context struct column
        """
        return df.withColumn(
            col_name,
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
        )

    @staticmethod
    def add_request_context_struct_v2(
        df: DataFrame, col_name: str = "request_context"
    ) -> DataFrame:
        """
        Adds a structured column of request context to the DataFrame.
        This column is composed of nested fields from the "requestContext" structure,
        and the `requestContext` structure definition is from the corresponding SXM services events.
        - Args:
            - df (DataFrame): A DataFrame with the `requestContext` struct column
            - col_name (str): The column name of the request context struct. Default value
                assigned is "request_context"
        - Returns:
            - DataFrame: A DataFrame with the request context struct column
        """
        return df.withColumn(
            col_name,
            F.struct(
                F.struct(
                    F.struct(
                        F.col(
                            "requestContext.origin.internalAgentAction.agentId"
                        ).alias("agent_id")
                    ).alias("internal_agent_action"),
                    F.struct(F.col("requestContext.origin.internalUserAction._")).alias(
                        "internal_user_action"
                    ),
                    F.struct(
                        F.col(
                            "requestContext.origin.internalSystemAction.systemName"
                        ).alias("system_name"),
                        F.col(
                            "requestContext.origin.internalSystemAction.description"
                        ).alias("description"),
                    ).alias("internal_system_action"),
                ).alias("origin"),
                F.col("requestContext.deviceId").alias("device_id"),
                F.col("requestContext.sessionId").alias("session_id"),
                F.col("requestContext.tokenId").alias("token_id"),
                F.col("requestContext.actionId").alias("action_id"),
                F.col("requestContext.identityId").alias("identity_id"),
                F.col("requestContext.accountId").alias("account_id"),
                F.col("requestContext.profileId").alias("profile_id"),
                F.col("requestContext.agentId").alias("agent_id"),
                F.struct(
                    F.col("requestContext.logicalClock.epoch").alias("epoch"),
                    F.col("requestContext.logicalClock.counter").alias("counter"),
                ).alias("logical_clock"),
            ),
        )