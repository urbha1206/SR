from datetime import datetime
from string import Template

import pyspark.sql.functions as F
import teradatasql
from data_eng_etl_lib.transform.domain_event_processor import DomainEventProcessor
from pydantic import validate_call
from pyspark.sql import SparkSession

from curated_legacy.config import Config, IncrementalConfig
from curated_legacy.db_connection import get_td_creds_json
from curated_legacy.loads import write_to_target_table
from curated_legacy.transformations import add_audit_columns, get_new_and_updated_rows
from curated_legacy.utils import get_max_process_time, get_previous_process_time
from curated_legacy.validations import check_for_duplicate_rows, find_nth


@validate_call
def run(config: Config | IncrementalConfig, s3_path: str, run_timestamp: str) -> None:
    spark = SparkSession.builder.appName(config.session.name).getOrCreate()

    td_creds_json = get_td_creds_json(spark)

    ts = datetime.fromisoformat(run_timestamp)
    new_s3_path = s3_path + f"{ts.year}-{ts.month:02d}-{ts.day:02d}/{ts.hour:02d}/"

    with open(config.read.query_file_path) as file:
        sql_template = Template(file.read())
    sql_query = sql_template.safe_substitute(s3_path=new_s3_path)

    if isinstance(config, IncrementalConfig):
        previous_process_time = get_previous_process_time(
            spark,
            config.read.job_metadata_location,
            config.read.default_init_process_ts,
        )
        sql_template_incr = Template(sql_query)
        sql_query = sql_template_incr.safe_substitute(process_ts=previous_process_time)

    teradata_connection = teradatasql.connect(
        host=td_creds_json["host"],
        user=td_creds_json["username"],
        password=td_creds_json["password"],
        database=config.read.database,
    )
    cursor = teradata_connection.cursor()
    cursor.execute(sql_query)
    teradata_connection.close()

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
        .transform(get_new_and_updated_rows, target_df, config.read.primary_key)
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
