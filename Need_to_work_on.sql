/*** 
/*** V2 ***/
Delta:
1. legacy_subscription_scd_stg:
   Legacy subscriptions can chnage the individual id with no changes in sync's atlas subscriptions.
   Atlas subscriptions sync from Legacy is real time. But TD legacy subscription data is refreshed once a day.
   To cover this delay from Legacy, we will check for legacy subscription changes using v_from_timestamp & to_timestamp.
   Atlas subscriptions will not be checked for any time.
   
2. atlas_subscription_scd_stg:
   In case of atlas subscriptions delay (more than a day), legacy subscriptions might already have been processed.
   To cover this delay from Atlas, we will check for Atlas subscription changes using v_from_timestamp & to_timestamp.
   Legacy subscriptions will not be checked for any time
   
If possible, you can cover these 2 steps (2 stg tables) into one ***/

drop table if exists  sandbox_prod.sbosco.legacy_subscription_stg;
drop table if exists  sandbox_prod.sbosco.legacy_subscription_scd_stg;
create table  sandbox_prod.sbosco.legacy_subscription_scd_stg
select
subscription_id, 
identity_id, 
source_external_reference, /*** V2 ***/
individual_id,
account_id,
household_id,

valid_from_date_hour, -- should switch to TD vehicle device for production.
/*** V2 ***/
coalesce(lead(valid_from_date_hour) OVER (PARTITION BY identity_id ORDER BY valid_from_date_hour), cast('9999-12-31' as timestamp)) as valid_to_date_hour,

is_deleted

--atlas_sub: identify new records or the records with chnages from prior update
-- get legacy account & individual id
from 
(
    select
    atlas_sub.subscription_id, 
    atlas_sub.identity_id, 
    atlas_sub.source_external_reference, /*** V2 ***/
    legacy_sub.individual_id,
    legacy_sub.account_id,
    legacy_ind.household_id,
    
    atlas_sub.valid_from_date_hour, 
    atlas_sub.isDeleted as is_deleted,

    case when lag(struct(atlas_sub.identity_id, legacy_sub.account_id, legacy_sub.individual_id)) 
        over(partition by atlas_sub.identity_id ORDER BY valid_from_date_hour)
        = struct(atlas_sub.identity_id, legacy_sub.account_id, legacy_sub.individual_id) 
    then false 
    else true 
    end as is_new_change

    from sandbox_prod.data_eng.vehicle_device_dim_full legacy_sub
    
    join curated_prod.legacy.dim_subscription dim_sub
    on dim_sub.sms_sbscrptn_id = legacy_sub.subscription_id
    and dim_sub.is_latest_rec_ind = 'Y'

    --get atlas subscriptions
    join (   --cleansed events deduping multiple updates with same timestamp
        --since only legacy subcriptions are checked for exists, no need to track subscription updates.
        select subscriptionId as subscription_id, owner.identityId as identity_id, state,  
        case when source.sms is not null then source.sms.externalReference
            when source.zuora is not null then source.zuora.externalReference
            when source.internal is not null then source.internal.externalReference
            else 'unknown'
        end as billing_platform_external_reference,
        replace(replace(replace(replace(replace(billing_platform_external_reference, 'US_'), 'MX_'), 'PR_'), 'NL_'), '0.0.0.1 /service/subscription_') as source_external_reference,
        valid_from_date_hour, isDeleted, envelopeTime 
        from
        ( 
        select subscriptionId, owner, state, source, envelopeTime,
        date_trunc('HOUR',from_utc_timestamp(envelopeTime,'GMT-5')) as valid_from_date_hour, 
        false as isDeleted from cleansed_prod.commerce.subscription_created_events
        where isTest = false
        and (source.sms is not null or source.zuora is not null or source.charon is not null or source.internal is not null)
        union all
        select subscriptionId, owner, state, source, envelopeTime,
        date_trunc('HOUR',from_utc_timestamp(envelopeTime,'GMT-5')) as valid_from_date_hour, 
        true as isDeleted from cleansed_prod.commerce.subscription_deleted_events
        where isTest = false
        and (source.sms is not null or source.zuora is not null or source.charon is not null or source.internal is not null)
        )

  ) atlas_sub
  on  dim_sub.short_sbscrptn_id = atlas_sub.source_external_reference
  
  left join sandbox_prod.data_eng.individual_dim_full legacy_ind
  on legacy_sub.individual_id = legacy_ind.individual_id

  where legacy_sub.account_id is not null
  and legacy_sub.etl_metadata_struct.etl_update_est_timestamp >= from_utc_timestamp(v_from_timestamp,'GMT-5')
  and legacy_sub.etl_metadata_struct.etl_update_est_timestamp <  from_utc_timestamp(v_to_timestamp,'GMT-5')
  --this need to be adjusted to valid from & to timestamp

  --make sure to get one subscription per identity + update timestamp
  --if identity has multiple subscriptions updated at same time, use state
  --make sure to take the latest record from legacy sub (within atlas sub validity)
  qualify row_number() over(partition by atlas_sub.identity_id, atlas_sub.valid_from_date_hour
                            order by envelopeTime desc, isDeleted desc, 
                                     case when state in ('active') then 1
                                          when state in ('paused', 'past_due', 'pending_cancellation') then 2
                                          else 3
                                      end, 
                                      --legacy_sub.valid_to_est_timestamp desc, 
                                      dim_sub.sbscrptn_key_id desc,
                                      legacy_sub.etl_metadata_struct.etl_update_est_timestamp desc) = 1
)
where is_new_change = true or is_deleted = true
;

select count(*) from sandbox_prod.sbosco.legacy_subscription_scd_stg;
