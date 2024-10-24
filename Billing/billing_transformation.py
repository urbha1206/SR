
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame

def transform_billing_profile_scd(billing_profile_df: DataFrame) -> DataFrame:
     return billing_profile_df.select(
        F.col("id").alias("id"),
        F.col("identityId").alias("identity_id"),
        F.col("tenant").alias("tenent_id"),
        F.col("billingProfileName").alias("billing_profile_name"),
        F.col("serviceAddress").alias("service_address"),
        F.col("envelopeTime").alias("envelope_time"),
        F.from_utc_timestamp("envelopeTime", "GMT-5").alias("valid_from_est_timestamp"),
        "source_timestamp_struct",
        F.col("valid_to_est_timestamp").alias("valid_to_est_timestamp"),
        F.date_trunc('HOUR',"valid_from_est_timestamp").alias("valid_from_date_hour"),
        F.col("is_current").alias("is_current"),
        F.col("etl_metadata_struct").alias("etl_metadata_struct"),
       
        #F.col("dsrDetail").alias("dsr_detail"),

    ).where(F.col("isTest") == "false")