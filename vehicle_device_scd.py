import logging
import typing

from pydantic import validate_call
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from curated_user.config import Config
from curated_user.loads import write_scd2_merge_all_events
from curated_user.scd2_transformations import (
    build_scd2_new_events,
    consolidate_with_target_df,
    ignore_sequential_unchanged_events,
    process_deleted_events,
)
from curated_user.vehicle_device_scd.vehicle_device_scd_transformations import (
    transform_curated_audit_scd_attributes,
    transform_current_subscription_attributes,
    transform_device_dim_df_join_conditions,
    transform_device_service_status,
    transform_identity_id,
    transform_linked_attributes,
    transform_trial_attributes,
    transform_trial_identity_id,
    transform_vehile_device_scd,
)


@validate_call
def run(config: Config) -> None:
    """
    This function performs several ETL steps, including:
    - Reading sources device, vehicle, profile and subscription data based on configuration
        and filtering it by the latest snapshot date and vehcile scd current records.
    - Applying a series of transformations to prepare the data for
    SCD2 merging, including audit column additions,
    and filtering out unchanged data.
    - Merging the transformed data into the target Delta table.

    Args:
        config (Config): configuration object with relevant job parameters
    """
    logger = logging.getLogger()

    logger.info("Read options: %s", config.read)
    logger.info("Write options: %s", config.write)

    spark = SparkSession.builder.appName(config.write.queryName).getOrCreate()

    from_tables = typing.cast(dict[str, list[str]], config.read.fromTables)

    device_dim_df = spark.read.table(from_tables["device_dim"][0])
    vehicle_scd_df = spark.read.table(from_tables["vehicle_scd"][0])

    profile_dim_df = spark.read.table(from_tables["profile_dim"][0])
    subscription_snap_df = spark.read.table(from_tables["subscription_snap"][0])

    target_table_df = spark.read.table(config.write.toTable)

    vechile_device_scd_df = (
        device_dim_df.alias("dvc")
        .transform(
            transform_device_dim_df_join_conditions,
            vehicle_scd_df,
            profile_dim_df,
            subscription_snap_df,
        )
        .transform(transform_device_service_status)
        .transform(transform_trial_identity_id)
        .transform(transform_trial_attributes)
        .transform(transform_identity_id)
        .transform(transform_linked_attributes)
        .transform(transform_current_subscription_attributes)
        .transform(transform_curated_audit_scd_attributes)
        .withColumn("is_current", F.lit(True))
        .transform(transform_vehile_device_scd)
    )

    vehicle_device_scd2_df = (
        vechile_device_scd_df.transform(
            consolidate_with_target_df,
            target_table_df,
            config.write.targetTableUniqueKey,
            False,
        )
        .select(*target_table_df.columns)
        .transform(
            ignore_sequential_unchanged_events,
            config.write.targetTableUniqueKey,
            config.write.targetTableOrderBy,
            config.write.targetHashExcludeColumns,
        )
        .transform(
            build_scd2_new_events,
            config.write.targetTableUniqueKey,
            config.write.targetTableOrderBy,
        )
        .transform(process_deleted_events)
    )

    write_scd2_merge_all_events(spark, vehicle_device_scd2_df, config)


def main() -> None:
    import fire

    fire.Fire(run)
