import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.sql.functions import col


def get_element_id(df: DataFrame) -> DataFrame:
    """
    Applies the necessary transformations to extract 'id' fields from the 'externalIds' array and
    adds them as new columns in the DataFrame.

    This function filters the 'externalIds' array based on specific 'source' values ('oemOwner',
    'oem', and 'device') and extracts the corresponding 'id' for each, creating new columns
    'oem_owner_id', 'oem_id', and 'device_id'.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The transformed DataFrame with the new columns.
    """
    return (
        df.withColumn(
            "oem_owner_id",
            F.expr(
                "try_element_at(filter(externalIds, x -> x.source = 'oemOwner'), 1).id"
            ),
        )
        .withColumn(
            "oem_id",
            F.expr("try_element_at(filter(externalIds, x -> x.source = 'oem'), 1).id"),
        )
        .withColumn(
            "device_id",
            F.expr(
                "try_element_at(filter(externalIds, x -> x.source = 'device'), 1).id"
            ),
        )
    )


def transform_vehicle_attributes(df: DataFrame) -> DataFrame:
    """
    Transforms the `vehicleAttributes` column into `vehicle_type` and  `automotive_struct`


    Args:
        df (DataFrame): Input DataFrame containing the `vehicleAttributes` column.

    Returns:
        DataFrame: Transformed DataFrame with `vehicle_type` and `automotive_struct` columns.
    """

    return df.withColumn(
        "vehicle_type",
        F.when(F.col("vehicleAttributes.automotive").isNotNull(), F.lit("automotive")),
    ).withColumn("automotive_struct", F.col("vehicleAttributes.automotive"))


def transform_special_attributes(df: DataFrame) -> DataFrame:
    """
    Transforms the `specialAttributes.tesla` field and adds a struct containing `delivery_state`
    and `vehicle_sale_type` as a new column in the DataFrame.

    This function processes Tesla-specific attributes by extracting key details and creating
    a simpler `tesla_special_attribute_struct`. It includes checks to handle null values gracefully.
    """

    tesla_special_attribute_struct = F.when(
        F.col("specialAttributes").isNotNull()
        & F.col("specialAttributes.tesla.deliveryState").isNotNull(),
        F.struct(
            F.map_keys(
                F.from_json(
                    F.to_json(F.col("specialAttributes.tesla.deliveryState")),
                    "MAP<STRING, STRING>",
                )
            )[0].alias("delivery_state"),
            F.when(
                F.map_keys(
                    F.from_json(
                        F.to_json(F.col("specialAttributes.tesla.deliveryState")),
                        "MAP<STRING, STRING>",
                    )
                )[0]
                == "delivered",
                F.col("specialAttributes.tesla.deliveryState.delivered.vehicleType"),
            )
            .otherwise(F.lit(None))
            .alias("vehicle_sale_type"),
        ),
    ).otherwise(
        F.struct(
            F.lit(None).alias("delivery_state"), F.lit(None).alias("vehicle_sale_type")
        )
    )

    return df.withColumn(
        "tesla_special_attribute_struct", tesla_special_attribute_struct
    ).drop("specialAttributes")


def transform_legal_terms(df: DataFrame) -> DataFrame:
    """
    Transforms the `legalTerms` field, which is an array of structs, into a single struct
    containing various legal terms attributes, where each field is an array of values.

    Args:
        df (DataFrame): Input DataFrame containing the `legalTerms` array.

    Returns:
        DataFrame: DataFrame with an additional column containing the `legal_terms_struct`.
    """

    return df.withColumn(
        "legal_terms_struct",
        F.struct(
            F.array_sort(
                F.transform(F.col("legalTerms"), lambda x: x.appVersion)
            ).alias("app_version"),
            F.array_sort(
                F.transform(F.col("legalTerms"), lambda x: x.countryCode)
            ).alias("country_code"),
            F.array_sort(
                F.transform(F.col("legalTerms"), lambda x: x.consentType)
            ).alias("consent_type"),
            F.array_sort(F.transform(F.col("legalTerms"), lambda x: x.language)).alias(
                "language"
            ),
            F.array_sort(F.transform(F.col("legalTerms"), lambda x: x.version)).alias(
                "version"
            ),
            F.array_sort(F.transform(F.col("legalTerms"), lambda x: x.platform)).alias(
                "device_platform"
            ),
            F.array_sort(
                F.transform(
                    F.col("legalTerms"), lambda x: x.acceptedAt.cast(T.TimestampType())
                )
            ).alias("accepted_est_timestamp"),
        ),
    ).drop("legalTerms")


def transform_vehicle_events(src_df: DataFrame) -> DataFrame:
    """
    Transforms the vehicle events from the input DataFrame and formats all vehicle attributes
    to be loaded into the vehicle SCD dimension table

    This function selects and renames relevant columns from the source DataFrame, aligning
    them with the target schema required for SCD processing. It ensures that all necessary
    attributes are included for accurate historical tracking.

    Args:
        src_df (DataFrame): Input DataFrame containing vehicle info from the vehicle evensts table

    Returns:
        DataFrame: Transformed DataFrame with all required device attributes.
    """

    return src_df.select(
        col("vehicleId").alias("vehicle_id"),
        col("partner").alias("partner_name"),
        col("source").alias("vehicle_source"),
        "oem_owner_id",
        "oem_id",
        "device_id",
        col("countryOfSale").alias("sale_country_code"),
        col("activationDate").alias("activation_date"),
        "vehicle_type",
        col("userOptedOut").alias("has_user_opted_out"),
        "automotive_struct",
        "tesla_special_attribute_struct",
        "legal_terms_struct",
        "source_timestamp_struct",
        "valid_from_est_timestamp",
        "valid_to_est_timestamp",
        "is_current",
        "request_context_struct",
        "etl_metadata_struct",
        "dsrDetail",
    )
