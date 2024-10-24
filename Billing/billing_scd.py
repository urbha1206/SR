
import logging
import typing

from pydantic import validate_arguments
from pyspark.sql import SparkSession, Window
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def get_events_in_interval(
        df: DataFrame,
        start_date: str | None = None,
        end_date: str | None = None,
        event_ts_col: str = "envelopeTime",
        tz: str = "UTC-5",
    ) -> DataFrame:
    return df.where(
                (
                    F.from_utc_timestamp(event_ts_col, tz)>=start_date
                )
                & (
                    F.from_utc_timestamp(event_ts_col, tz)<= end_date
                )
            )

def get_events_until_current(df: DataFrame,time_delay_hours:int) -> DataFrame:
   
    return df.where(
        F.from_utc_timestamp("envelopeTime", "GMT-5")
        < datetime.now() - timedelta(hours=time_delay_hours)
    )    

class LatestEventIntervalType(str, Enum):
    DAY = "DAY"
    HOUR = "HOUR"
    EVENT = "EVENT"


def get_latest_events_for_each_interval_v2(
        df: DataFrame,
        event_ts_col: str,
        partition_by: list[str],
        order_by: list[tuple[str, str]],
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
            - order_by (list[tuple[str, str]]): The list of columns to order by, along with the ordering direction ("desc" or "asc").
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
                        *[F.desc(c) if sort_order == "desc" else F.asc(c)
                             for c, sort_order in order_by]
                    )
                ),
            )
            .where(F.col(rank_col) == 1)
            .drop(rank_col, interval_col)
        )




@validate_arguments
def run(config: Config | Scd2BackfillConfig) -> None:
    """
    This function performs several ETL steps, including:
    - Reading source channel data based on configuration and filtering it by the provided run date.
    - Joining channel & category and create grouped struct
    - Applying a series of transformations to prepare the data for
    SCD2 merging, including audit column additions,
    and filtering out unchanged data.
    - Merging the transformed data into the target Delta table.

    Args:
        config (Config): configuration object with relevant job parameters
        run_date (str): data cdc filter date
    """
    logger = logging.getLogger()

    spark = SparkSession.builder.appName(config.write.queryName).getOrCreate()
    target_table_df = spark.table(config.write.toTable)
    from_tables = typing.cast(dict[str, list[str]], config.read.fromTables)

    logger.info("Read options: %s", config.read)
    logger.info("Write options: %s", config.write)

    window_spec = Window.partitionBy(config.read.sourcePartitionKey)
    # startDate=datetime.now() - timedelta(days=3)
    # endDate=datetime.now() - timedelta(hours=-2)
    src_dfs = [
        df.transform(
            DomainEventReader.get_events_by_date,
            config.seqBackfillOptions.startDate,
            config.seqBackfillOptions.endDate,
        )

        if isinstance(config, Scd2BackfillConfig)
        else df.transform(get_events_until_current,config.read.time_delay_hours)
        for df in list(map(spark.read.table, from_tables["billing_profile_events"]))
    ]

    final_df = (
       DomainEventProcessor.union_dfs(src_dfs)
              .transform(
            DomainEventProcessor.add_audit_columns_v2, config.read.deleteEntityPattern
        )
           .transform(
            get_latest_events_for_each_interval_v2,
            config.read.eventTimestampCol,
             config.read.sourcePartitionKey,
            #config.read.orderBy,#
            [("envelopeTime", "desc"), ("etl_metadata_struct.is_deleted", "asc")],
            config.read.latestEventIntervalType,
        )
        #    .transform(
        #     get_latest_events_for_each_interval_v2,
        #     config.read.eventTimestampCol,
        #     config.read.sourcePartitionKey,
        #     config.read.orderBy,
        #     config.read.latestEventIntervalType,
        # )

        .transform(DomainEventProcessor.add_scd2_audit_columns)
        .transform(
            DomainEventProcessor.add_source_timestamp_struct,
            "createdAt",
            "lastUpdatedAt",
        )
        .transform(transform_billing_profile_scd)
        .select(*target_table_df.columns)
        .transform(
            DomainEventProcessor.consolidate_with_scd2_target_df,
            target_table_df,
            config.write.targetTableUniqueKey,
            is_full_table_reload=isinstance(config, Scd2BackfillConfig),
        )
        .transform(
            DomainEventProcessor.ignore_sequential_unchanged_events,
            config.write.targetTableUniqueKey,
            config.write.targetTableOrderBy,
            config.write.targetHashExcludeColumns,
        )
        .transform(
            DomainEventProcessor.build_scd2_events,
            config.write.targetTableUniqueKey,
            config.write.targetTableOrderBy,
        )
        # For isdeleted
     .transform(DomainEventProcessor.process_deleted_events)
    )

    #final_df.display()
    if isinstance(config, Scd2BackfillConfig):
        final_df.write.saveAsTable(config.write.toTable, mode="overwrite")
    else:
        write_scd2_merge_all_events(spark, final_df, config)

def main() -> None:
    import fire

    fire.Fire(run)

