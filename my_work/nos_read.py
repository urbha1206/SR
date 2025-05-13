from datetime import datetime
from string import Template
from typing import Optional

import teradatasql
from pydantic import validate_call
from pyspark.sql import SparkSession

from curated_legacy.config import Config, IncrementalConfig
from curated_legacy.db_connection import get_td_creds_json
from curated_legacy.utils import get_previous_process_time


@validate_call
def run(
    config: Config | IncrementalConfig,
    s3_path: str,
    run_timestamp: str,
    retry_count: Optional[int] = None,
) -> None:
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


def main() -> None:
    import fire

    fire.Fire(run)


if __name__ == "__main__":
    main()
