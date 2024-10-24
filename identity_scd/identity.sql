drop table if exists  sandbox_prod.sbosco.identity_scd_stg;
create table  sandbox_prod.sbosco.identity_scd_stg
select
identityId as identity_id,
email_address,
phone_number,
/*** V2 ***/
idt_create_est_timestamp,
idt_update_est_timestamp,

valid_from_date_hour,
/*** V2 ***/
coalesce(lead(valid_from_date_hour) OVER (PARTITION BY identityId ORDER BY valid_from_date_hour), cast('9999-12-31' as timestamp)) as valid_to_date_hour,


case when isDeleted = false and valid_to_date_hour = '9999-12-31 00:00:00.000' then true else false end as is_current,
isDeleted as is_deleted

from --identify new records or the records with chnages from prior update
(
    select identityId, email_address, phone_number, 
    from_utc_timestamp(createdAt,'GMT-5') as idt_create_est_timestamp, /*** V2 ***/
    from_utc_timestamp(lastUpdatedAt,'GMT-5') as idt_update_est_timestamp, /*** V2 ***/
    valid_from_date_hour, isDeleted,
    case when lag(struct(identityId, email_address, phone_number)) 
        over(partition by identityId ORDER BY envelopeTime)
        = struct(identityId, email_address, phone_number) 
    then false 
    else true 
    end as is_new_change

    from 
    (  --cleansed events deduping multiple updates with same timestamp
        select * from
        ( 
        select identityId, coalesce(handle.emailAddress, contact.emailAddress) as email_address,
        contact.PhoneNumber as phone_number, createdAt, lastUpdatedAt, envelopeTime,
        from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp,
        date_trunc('HOUR',valid_from_est_timestamp) as valid_from_date_hour, 
        false as isDeleted from cleansed_prod.user_services.identity_created_events
        where isTest = false
        and envelopeTime >= v_from_timestamp and envelopeTime < v_to_timestamp /*** V2 ***/
        union all
        select identityId, coalesce(handle.emailAddress, contact.emailAddress) as email_address,
        contact.PhoneNumber as phone_number, createdAt, lastUpdatedAt, envelopeTime,
        from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp,
        date_trunc('HOUR',valid_from_est_timestamp) as valid_from_date_hour, 
        false as isDeleted from cleansed_prod.user_services.identity_updated_events
        where isTest = false
        and envelopeTime >= v_from_timestamp and envelopeTime < v_to_timestamp /*** V2 ***/
        union all
        select identityId, coalesce(handle.emailAddress, contact.emailAddress) as email_address,
        contact.PhoneNumber as phone_number, createdAt, lastUpdatedAt, envelopeTime,
        from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp,
        date_trunc('HOUR',valid_from_est_timestamp) as valid_from_date_hour, 
        true as isDeleted from cleansed_prod.user_services.identity_deleted_events
        where isTest = false
        and envelopeTime >= v_from_timestamp and envelopeTime < v_to_timestamp /*** V2 ***/
        )
        qualify row_number() over (PARTITION BY identityId, valid_from_date_hour ORDER BY envelopeTime desc, isDeleted desc) = 1
        --if create/update and delete are with same envelopetime, Deleted event will be taken.
    ) 
    
)
where is_new_change = true or isDeleted = true;

select count(*) from sandbox_prod.sbosco.identity_scd_stg;
--124092746