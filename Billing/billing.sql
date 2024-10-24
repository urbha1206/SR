drop table if exists  sandbox_prod.sbosco.billing_profile_scd_stg;
create table  sandbox_prod.sbosco.billing_profile_scd_stg
select
id as billing_profile_id,
identityId as identity_id,
tenant as tenant_id, 
billingProfileName.firstAndLastName.firstName as first_name,
billingProfileName.firstAndLastName.lastName as last_name,
struct(serviceAddress.city, serviceAddress.state, serviceAddress.zipCode as zip_code, serviceAddress.addressLine1 address_line_1, serviceAddress.addressLine2 as address_line_2,  serviceAddress.country) as service_address_struct,

valid_from_date_hour,
/*** V2 ***/
coalesce(lead(valid_from_date_hour) OVER (PARTITION BY identityId ORDER BY valid_from_date_hour), cast('9999-12-31' as timestamp)) as valid_to_date_hour,

isDeleted as is_deleted

from --identify new records or the records with chnages from prior update
(
    select id, identityId, tenant, billingProfileName, serviceAddress, valid_from_date_hour, isDeleted,
    case when lag(struct(identityId, tenant, billingProfileName, serviceAddress)) 
        over(partition by identityId ORDER BY envelopeTime)
        = struct(identityId, tenant, billingProfileName, serviceAddress) 
    then false 
    else true 
    end as is_new_change

    from 
    (  --cleansed events deduping multiple updates with same timestamp
        select * from
        ( 
        select id, identityId, tenant, billingProfileName, serviceAddress, envelopeTime,
        from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp,
        date_trunc('HOUR',valid_from_est_timestamp) as valid_from_date_hour, 
        false as isDeleted from cleansed_prod.commerce.billing_profile_created_events
        where isTest = false
        and envelopeTime >= v_from_timestamp and envelopeTime < v_to_timestamp /*** V2 ***/
        union all
        select id, identityId, tenant, billingProfileName, serviceAddress, envelopeTime,
        from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp,
        date_trunc('HOUR',valid_from_est_timestamp) as valid_from_date_hour, 
        false as isDeleted from cleansed_prod.commerce.billing_profile_updated_events
        where isTest = false
        and envelopeTime >= v_from_timestamp and envelopeTime < v_to_timestamp /*** V2 ***/
        union all
        select id, identityId, tenant, billingProfileName, serviceAddress, envelopeTime,
        from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp,
        date_trunc('HOUR',valid_from_est_timestamp) as valid_from_date_hour, 
        true as isDeleted from cleansed_prod.commerce.billing_profile_deleted_events
        where isTest = false
        and envelopeTime >= v_from_timestamp and envelopeTime < v_to_timestamp /*** V2 ***/
        )
        qualify row_number() over (PARTITION BY identityId, valid_from_date_hour ORDER BY envelopeTime desc, isDeleted) = 1
        --partition by identityid and order by isDeleted because we see 2 billing profiles with same timestamp, one updated and one deleted
        --multiple billing profiles in sequence is supported, but updating one and deleting another one at the same time, creates issues in later delta merge.
    ) 
    
)
where is_new_change = true or isDeleted = true;

select count(*) from sandbox_prod.sbosco.billing_profile_scd_stg;