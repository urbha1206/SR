import logging
import typing

from data_eng_etl_lib.read.domain_event_reader import DomainEventReader
from data_eng_etl_lib.transform.domain_event_processor import DomainEventProcessor
from pydantic import validate_call
from pyspark.sql import SparkSession

from curated_user.config import Config, Scd2BackfillConfig
from curated_user.loads import write_scd2_merge_all_events
from curated_user.transformations import add_request_context_struct
from curated_user.vehicle_scd.vehicle_scd_transformations import (
    get_element_id,
    transform_legal_terms,
    transform_special_attributes,
    transform_vehicle_attributes,
    transform_vehicle_events,
)


@validate_call
def run(config: Config | Scd2BackfillConfig, run_date: str) -> None:
    """
    This function performs several ETL steps, including:
    - Reading source  data based on configuration and filtering it by the provided run date.
    - Applying a series of transformations to prepare the data for
    SCD2 merging, including audit column additions,
    and filtering out unchanged data.
    - Merging the transformed data into the target Delta table.

    Args:
        config (Config): configuration object with relevant job parameters
        run_date (str): data cdc filter date
    """
    logger = logging.getLogger()

    logger.info("Read options: %s", config.read)
    logger.info("Write options: %s", config.write)

    spark = SparkSession.builder.appName(config.write.queryName).getOrCreate()
    target_table_df = spark.table(config.write.toTable)
    from_tables = typing.cast(dict[str, list[str]], config.read.fromTables)

    src_dfs = [
        df.transform(
            DomainEventReader.get_events_by_date,
            config.seqBackfillOptions.startDate,
            config.seqBackfillOptions.endDate,
        )
        if isinstance(config, Scd2BackfillConfig)
        else df.transform(DomainEventReader.get_events_by_date, run_date)
        for df in list(map(spark.read.table, from_tables["vehicle_events"]))
    ]

    final_df = (
        DomainEventProcessor.union_dfs(src_dfs)
        .transform(
            DomainEventProcessor.get_latest_events_for_each_interval,
            config.read.eventTimestampCol,
            config.read.sourcePartitionKey,
            config.read.orderBy,
            config.read.latestEventIntervalType,
        )
        .transform(
            DomainEventProcessor.add_audit_columns_v2, config.read.deleteEntityPattern
        )
        .transform(DomainEventProcessor.add_scd2_audit_columns, "lastupdatedAt")
        .transform(get_element_id)
        .transform(transform_vehicle_attributes)
        .transform(transform_special_attributes)
        .transform(transform_legal_terms)
        .transform(
            DomainEventProcessor.add_source_timestamp_struct,
            create_timestamp_col="createdAt",
            update_timestamp_col="lastUpdatedAt",
        )
        .transform(add_request_context_struct)
        .transform(transform_vehicle_events)
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
        .transform(DomainEventProcessor.process_deleted_events)
    )

    if isinstance(config, Scd2BackfillConfig):
        final_df.write.saveAsTable(config.write.toTable, mode="overwrite")
    else:
        write_scd2_merge_all_events(spark, final_df, config)


def main() -> None:
    import fire

    fire.Fire(run)
