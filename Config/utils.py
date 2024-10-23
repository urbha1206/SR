from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType


def get_previous_process_time(
    spark: SparkSession, job_metadata_location: str | None
) -> datetime | None:
    """
    Get the timestamp since the last time the process ran to enable incremental
    reads, pulling this value from the metadata location specified in the config
    Args:
        spark (SparkSession): A Spark session
        job_metadata_location (str | None): A string for a job metadata location
    Returns:
        datetime | None: a datetime object pulled from the metadata indicating last process time
    """
    if DeltaTable.isDeltaTable(spark, job_metadata_location):
        previous_process_time: datetime | None = (
            spark.read.load(job_metadata_location)
            .select("processTimestamp")
            .collect()[0][0]
        )
    else:
        previous_process_time = None

    return previous_process_time


def log_process_timestamp(
    spark: SparkSession,
    job_metadata_location: str | None,
    processed_time: datetime,
    date_time: datetime | None,
) -> None:
    """
    This function logs the last processed timestamp to the metadata table
    Args:
        spark (SparkSession): the spark session
        job_metadata_location: the s3 path for the metadata that stores last processing time
        processed_time (datetime): the last processing time
        date_time (datetime): this will be used if the previous processed time is None to set the last processed time
    Returns:
        None
    """
    # TODO: maybe add a unit test to ensure src_processed_time_df (or the results
    #    of the function that creates it) always returns 1 row
    if processed_time is None:
        if date_time is not None:
            processed_time_df = spark.createDataFrame(
                [date_time], TimestampType()
            ).toDF("processTimestamp")
    else:
        processed_time_df = spark.createDataFrame(
            [processed_time], TimestampType()
        ).toDF("processTimestamp")

    processed_time_df.write.format("delta").mode("overwrite").save(
        job_metadata_location
    )
