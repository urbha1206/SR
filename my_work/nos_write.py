from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
from data_eng_etl_lib.transform.domain_event_processor import DomainEventProcessor
from pydantic import validate_call
from pyspark.sql import SparkSession

from curated_legacy.config import Config, IncrementalConfig
from curated_legacy.loads import write_to_target_table
from curated_legacy.transformations import add_audit_columns
from curated_legacy.utils import get_max_process_time
from curated_legacy.validations import check_for_duplicate_rows, find_nth


@validate_call
def run(
    config: Config | IncrementalConfig,
    s3_path: str,
    run_timestamp: str,
    retry_count: Optional[int] = None,
) -> None:
    spark = SparkSession.builder.appName(config.session.name).getOrCreate()

    ts = datetime.fromisoformat(run_timestamp)
    new_s3_path = s3_path + f"{ts.year}-{ts.month:02d}-{ts.day:02d}/{ts.hour:02d}/"
    index = find_nth(new_s3_path, "/", 5)
    volume = "/Volumes" + new_s3_path[index:]
    source_df = spark.read.parquet(volume)

    if config.read.remove_duplicate_order_by:
        source_df = source_df.transform(
            DomainEventProcessor.get_top_rank_events,
            config.read.primary_key,
            [
                (F.asc(c) if config.read.remove_duplicate_order_by[c] else F.desc(c))
                for c in config.read.remove_duplicate_order_by
            ],
        )
    check_for_duplicate_rows(source_df, config.read.primary_key)

    target_df = spark.read.table(config.write.to_table)
    target_schema = target_df.drop("etl_metadata_struct").schema
    col_map = {}
    for i in target_schema:
        col_map[i.name] = F.col(i.name).cast(i.dataType)

    updates_df = (
        source_df.transform(add_audit_columns)
        .withColumns(col_map)
        .select(*target_df.columns)
    )

    write_to_target_table(
        spark,
        updates_df,
        target_df,
        config.write.to_table,
        config.read.primary_key,
        config.write.mode,
    )

    if isinstance(config, IncrementalConfig):
        process_time_df = get_max_process_time(source_df, config.read.process_ts_col)
        process_time = process_time_df.collect()[0]["processTimestamp"]
        if process_time is not None:
            process_time_df.write.format("delta").mode("overwrite").save(
                config.read.job_metadata_location
            )


def main() -> None:
    import fire

    fire.Fire(run)


if __name__ == "__main__":
    main()
