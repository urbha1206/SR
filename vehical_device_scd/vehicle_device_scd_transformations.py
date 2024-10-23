from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def get_latest_snapshot_date(df: DataFrame) -> str:
    """
    Retrieves the latest date from the 'snapshot_est_date' column in the provided PySpark DataFrame.

    Args:
        df (DataFrame): A PySpark DataFrame containing a 'snapshot_est_date' column with date or timestamp values.

    Returns:
        str: The latest date from the 'snapshot_est_date' column, formatted as a string.
    """
    snapshot_date = df.select(
        F.max(F.col("snapshot_est_date")).cast(StringType())
    ).collect()[0][0]
    return str(snapshot_date)


def transform_device_service_status(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding a new column `device_service_status` that categorizes
    the device's service status based on several conditions related to delivery state, legal terms
    acceptance, and subscription status.

    The `device_service_status` column is assigned one of the following values based on the conditions:

    - "demo": If the vehicle's delivery state is "nonDelivered".
    - "trial_eligible": If the vehicle's delivery state is "delivered" and legal terms have not been accepted.
    - "trial": If the vehicle's delivery state is "delivered" and legal terms have been accepted.
    - "trial_expired": If the trial subscription is closed, cancelled, or finished,
        and it is not linked to a subscription.
    - "selfpay": If the current subscription is active (not closed, cancelled, or finished).
    - "end": If the current subscription is closed, cancelled, or finished.
    - "unknown": If none of the above conditions are met.

    Args:
        df (DataFrame): Input DataFrame containing vehicle and subscription information.

    Returns:
        DataFrame: The transformed DataFrame with an additional `device_service_status` column.
    """
    return df.withColumn(
        "device_service_status",
        F.when(
            F.col("veh.tesla_special_attribute_struct.delivery_state")
            == "nonDelivered",
            "demo",
        )
        .when(
            (F.col("veh.tesla_special_attribute_struct.delivery_state") == "delivered")
            & (F.col("veh.legal_terms_struct.accepted_est_timestamp")[0].isNull()),
            "trial_eligible",
        )
        .when(
            (F.col("veh.tesla_special_attribute_struct.delivery_state") == "delivered")
            & (F.col("veh.legal_terms_struct.accepted_est_timestamp")[0].isNotNull()),
            "trial",
        )
        .when(
            (F.col("trial_sub.state").isin("closed", "cancelled", "finished"))
            & (F.col("trial_sub.linked_to_subscription_id").isNull()),
            "trial_expired",
        )
        .when(
            ~F.col("current_sub.state").isin("closed", "cancelled", "finished"),
            "selfpay",
        )
        .when(F.col("current_sub.state").isin("closed", "cancelled", "finished"), "end")
        .otherwise("unknown"),
    )


def transform_trial_attributes(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding several columns related to trial subscription attributes
    for vehicles that have been delivered.

    The function adds the following columns:

    - `trial_subscription_id`: The subscription ID of the trial, set if the vehicle's
        delivery state is "delivered".
    - `trial_subscription_start_est_timestamp`: The estimated start timestamp of the
        trial subscription, set if the vehicle's delivery state is "delivered".
    - `trial_subscription_end_est_timestamp`: The estimated end timestamp of the
        trial subscription, set if the vehicle's delivery state is "delivered".
    - `trial_subscription_state`: The state of the trial subscription, set if
        the vehicle's delivery state is "delivered".
    - `trial_state`: Categorizes the trial state based on the device service
        status and other attributes, set if the vehicle's delivery state is "delivered".

    Args:
        df (DataFrame): Input DataFrame containing vehicle and subscription information.

    Returns:
        DataFrame: The transformed DataFrame with additional columns related to trial subscription attributes.
    """
    return (
        df.withColumn(
            "trial_subscription_id",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("trial_sub.subscription_id"),
            ),
        )
        .withColumn(
            "trial_subscription_start_est_timestamp",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("trial_sub.subscription_timestamp_struct.start_est_timestamp"),
            ),
        )
        .withColumn(
            "trial_subscription_end_est_timestamp",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("trial_sub.current_phase_struct.phase_end_est_timestamp"),
            ),
        )
        .withColumn(
            "trial_subscription_state",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("trial_sub.state"),
            ),
        )
        .withColumn(
            "trial_state",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",  # Need to change this to tesla_special_struct
                F.when(
                    F.col("device_service_status") == "demo",
                    F.when(F.col("trial_identity_id").isNull(), "anonymous")
                    .when(F.col("trial_identity_id").isNotNull(), "claimed")
                    .when(
                        F.col("trial_sub.linked_to_subscription_id").isNotNull(),
                        "followon",
                    ),
                ),
            ),
        )
    )


def transform_trial_identity_id(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding a new column `trial_identity_id`, which extracts the identity
    ID from the `beneficiary_id_array_struct` field within the trial subscription data.

    The `trial_identity_id` column is populated by filtering the `beneficiary_id_array_struct` array
    for elements where the type is 'identity', and then selecting the `id` of the first matching element.

    Args:
        df (DataFrame): Input DataFrame containing trial subscription information, including the
                        `beneficiary_id_array_struct` field.

    Returns:
        DataFrame: The transformed DataFrame with an additional `trial_identity_id` column.
    """
    return df.withColumn(
        "trial_identity_id",
        F.expr(
            "try_element_at( filter(trial_sub.beneficiary_id_array_struct, x -> x.type = 'identity'), 1).id"
        ),
    )


def transform_identity_id(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding a new column `identity_id_transformed`,
    which assigns the value of `trial_identity_id` for vehicles that have been delivered.

    The `identity_id_transformed` column is populated with the value of `trial_identity_id`
    if the vehicle's delivery state is "delivered". Otherwise, it remains null.

    Args:
        df (DataFrame): Input DataFrame containing vehicle information, including
                        the `trial_identity_id` and delivery state.

    Returns:
        DataFrame: The transformed DataFrame with an additional `identity_id_transformed` column.
    """
    return df.withColumn(
        "identity_id_transformed",
        F.when(
            F.col("veh.tesla_special_attribute_struct.delivery_state") == "delivered",
            F.coalesce(F.col("current_sub.identity_id"), F.col("trial_identity_id")),
        ),
    )


def transform_linked_attributes(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding new columns related to subscription linking attributes,
    which are populated based on the delivery state of the vehicle.

    The function adds the following columns:
    - `linked_to_subscription_id_transformed`: The linked subscription ID if the vehicle is delivered.
    - `linked_to_subscription_state_transformed`: The state of the linked subscription if the vehicle is delivered.
    - `linked_to_subscription_create_est_timestamp_transformed`: The creation timestamp of the linked subscription
      if the vehicle is delivered.

    Args:
        df (DataFrame): Input DataFrame containing vehicle and subscription information.

    Returns:
        DataFrame: The transformed DataFrame with additional columns for linked subscription attributes.
    """
    return (
        df.withColumn(
            "linked_to_subscription_id_transformed",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("trial_sub.linked_to_subscription_id"),
            ),
        )
        .withColumn(
            "linked_to_subscription_state_transformed",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("linked_sub.state"),
            ),
        )
        .withColumn(
            "linked_to_subscription_create_est_timestamp_transformed",
            F.when(
                F.col("veh.tesla_special_attribute_struct.delivery_state")
                == "delivered",
                F.col("linked_sub.source_timestamp_struct.create_est_timestamp"),
            ),
        )
    )


def transform_current_subscription_attributes(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding columns related to the current subscription attributes,
    specifically for vehicles that have been delivered.

    The function adds the following columns:
    - `current_subscription_id`: The ID of the current subscription if the vehicle is delivered.
    - `current_subscription_state`: The state of the current subscription if the vehicle is delivered.

    Args:
        df (DataFrame): Input DataFrame containing vehicle and subscription information.

    Returns:
        DataFrame: The transformed DataFrame with additional columns for current subscription attributes.
    """
    return df.withColumn(
        "current_subscription_id",
        F.when(
            F.col("veh.tesla_special_attribute_struct.delivery_state") == "delivered",
            F.col("current_sub.subscription_id"),
        ),
    ).withColumn(
        "current_subscription_state",
        F.when(
            F.col("veh.tesla_special_attribute_struct.delivery_state") == "delivered",
            F.col("current_sub.state"),
        ),
    )


def transform_curated_audit_scd_attributes(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by adding columns related to the start and end timestamps for
    a slowly changing dimension (SCD) in a curated audit context.

    The function adds the following columns:
    - `valid_from_est_timestamp_transformed`: The earliest timestamp among various sources
        that indicates when the record became valid.
    - `valid_to_est_timestamp`: A fixed timestamp representing the end date of the record's
        validity, defaulted to "9999-12-31".

    Args:
        df (DataFrame): Input DataFrame containing vehicle, device, profile, and subscription information.

    Returns:
        DataFrame: The transformed DataFrame with additional columns for the valid from and valid to timestamps.
    """
    return df.withColumn(
        "valid_from_est_timestamp_transformed",
        F.least(
            F.col("veh.valid_from_est_timestamp"),
            F.col("dvc.source_timestamp_struct.update_est_timestamp"),
            F.coalesce(
                F.col("driver_prf.source_timestamp_struct.update_est_timestamp"),
                F.col("demo_prf.source_timestamp_struct.update_est_timestamp"),
            ),
            F.col("current_sub.source_timestamp_struct.update_est_timestamp"),
            F.col("linked_sub.source_timestamp_struct.update_est_timestamp"),
            F.col("trial_sub.source_timestamp_struct.update_est_timestamp"),
        ),
    ).withColumn(
        "valid_to_est_timestamp", F.to_timestamp(F.lit("9999-12-31"), "yyyy-MM-dd")
    )


def transform_device_dim_df_join_conditions(
    df: DataFrame,
    vehicle_scd_df: DataFrame,
    profile_dim_df: DataFrame,
    subscription_snap_df: DataFrame,
) -> DataFrame:
    """
    Performs a series of left joins between the input DataFrame and various other DataFrames to enrich
    the data with vehicle, profile, and subscription information, and applies filtering and ranking to
    select the most relevant records.

    The function joins the input DataFrame (`df`) with the following DataFrames:
    - `vehicle_scd_df`: Contains vehicle details.
    - `profile_dim_df`: Contains profile information (used for both demo and driver profiles).
    - `subscription_snap_df`: Contains subscription snapshot information (used for trial, linked,
        and current subscriptions).

    After joining, it filters the records to include only those where `veh.is_current` is `True`,
    and ranks the records by the start timestamp of the trial and current subscriptions.
    Only the top-ranked record for each device ID is retained.

    Args:
        df (DataFrame): The primary DataFrame containing device information.
        vehicle_scd_df (DataFrame): DataFrame with vehicle details.
        profile_dim_df (DataFrame): DataFrame with profile information.
        subscription_snap_df (DataFrame): DataFrame with subscription snapshots.

    Returns:
        DataFrame: The enriched and filtered DataFrame with additional columns and only the
            top-ranked records per device ID.
    """
    return (
        df.join(
            vehicle_scd_df.alias("veh"),
            F.col("dvc.vehicle_attribute_struct.vehicle_id") == F.col("veh.vehicle_id"),
            how="left",
        )
        .join(
            (profile_dim_df).alias("demo_prf"),
            F.col("demo_prf.device_id") == F.col("dvc.device_id"),
            how="left",
        )
        .join(
            profile_dim_df.alias("driver_prf"),
            F.col("driver_prf.oem_profile_id") == F.col("veh.oem_owner_id"),
            how="left",
        )
        .join(
            subscription_snap_df.alias("trial_sub"),
            (
                F.col("trial_sub.snapshot_est_date")
                == get_latest_snapshot_date(subscription_snap_df)
            )
            & (F.col("trial_sub.device_id") == F.col("dvc.device_id")),
            how="left",
        )
        .join(
            subscription_snap_df.alias("linked_sub"),
            (
                F.col("linked_sub.snapshot_est_date")
                == get_latest_snapshot_date(subscription_snap_df)
            )
            & (
                F.col("linked_sub.subscription_id")
                == F.col("trial_sub.linked_to_subscription_id")
            ),
            how="left",
        )
        .join(
            subscription_snap_df.alias("current_sub"),
            (
                F.col("current_sub.snapshot_est_date")
                == get_latest_snapshot_date(subscription_snap_df)
            )
            & (F.col("current_sub.identity_id") == F.col("linked_sub.identity_id")),
            how="left",
        )
        .filter(F.col("veh.is_current"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("dvc.device_id").orderBy(
                    F.col(
                        "trial_sub.subscription_timestamp_struct.start_est_timestamp"
                    ).desc(),
                    F.col(
                        "current_sub.subscription_timestamp_struct.start_est_timestamp"
                    ).desc(),
                )
            ),
        )
        .filter(F.col("rn") == 1)
    )


def transform_vehile_device_scd(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame to select and rename specific columns related to vehicle and device
    attributes, subscription details, and related metadata.

    This function selects a subset of columns from the input DataFrame and renames some columns for clarity.
    It includes information about the device, vehicle, and various subscription attributes, as well as metadata
    related to the vehicle's legal terms and service status.

    Args:
        df (DataFrame): The input DataFrame containing raw data related to devices, vehicles, and subscriptions.

    Returns:
        DataFrame: A transformed DataFrame with a specific set of columns and some renamed for clarity.
    """
    return df.select(
        F.col("dvc.device_id"),
        F.col("dvc.vehicle_attribute_struct.radio_id"),
        F.col("dvc.device_platform"),
        F.col("veh.vehicle_id"),
        F.col("veh.vehicle_type"),
        F.col("veh.partner_name"),
        F.col("veh.oem_owner_id"),
        F.col("veh.automotive_struct.vin"),
        F.col("veh.automotive_struct.make"),
        F.col("veh.automotive_struct.model"),
        F.col("veh.automotive_struct.year"),
        F.col("veh.tesla_special_attribute_struct.delivery_state"),
        F.col("veh.tesla_special_attribute_struct.vehicle_sale_type"),
        F.col("veh.sale_country_code"),
        F.col("demo_prf.profile_id").alias("demo_profile_id"),
        F.col("demo_prf.language_code").alias("demo_profile_language_code"),
        F.col("driver_prf.profile_id").alias("oem_owner_profile_id"),
        F.col("driver_prf.language_code").alias("oem_owner_profile_language_code"),
        F.col("device_service_status"),
        F.col("veh.legal_terms_struct.accepted_est_timestamp")[0].alias(
            "legal_terms_accept_est_timestamp"
        ),
        F.col("trial_subscription_id"),
        F.col("trial_subscription_start_est_timestamp"),
        F.col("trial_subscription_end_est_timestamp"),
        F.col("trial_subscription_state"),
        F.col("trial_state"),
        F.col("trial_identity_id"),
        F.col("identity_id_transformed").alias("identity_id"),
        F.col("linked_to_subscription_id_transformed").alias(
            "linked_to_subscription_id"
        ),
        F.col("linked_to_subscription_state_transformed").alias(
            "linked_to_subscription_state"
        ),
        F.col("linked_to_subscription_create_est_timestamp_transformed").alias(
            "linked_to_subscription_create_est_timestamp"
        ),
        F.col("current_subscription_id"),
        F.col("current_subscription_state"),
        F.col("valid_from_est_timestamp_transformed").alias("valid_from_est_timestamp"),
        F.col("valid_to_est_timestamp"),
        F.col("is_current"),
        F.col("veh.etl_metadata_struct"),
        F.col("veh.dsrDetail"),
    )
