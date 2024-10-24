%sql
drop table if exists sandbox_prod.sbosco.vehicle_scd;
create table sandbox_prod.sbosco.vehicle_scd
select 
  vehicleId as vehicle_id, 
  partner as partner_name,
  vehicleSource as vehicle_source,

  try_element_at( filter(externalIds, x -> x.source = 'oemOwner'), 1).id as oem_owner_id, 
  try_element_at( filter(externalIds, x -> x.source = 'oem'), 1).id as oem_id,
  try_element_at( filter(externalIds, x -> x.source = 'device'), 1).id as device_id,

  countryOfSale as sale_country_code,
  activationDate as activation_date,

  
  --vehicle attributes
  from_json(to_json(vehicleAttributes), 'MAP<STRING, STRING>') AS vehicleAttributes_map, -- not needed in prod table
  from_json(map_values(vehicleAttributes_map)[0],'MAP<STRING, STRING>') AS vehicleAttributes_value_map, -- not needed in prod table
  map_keys(vehicleAttributes_map)[0] as vehicle_type, -- automotive
  case when vehicle_type = 'automotive' then vehicleAttributes_value_map end as automotive_struct,
  
  --special attributes
  from_json(to_json(specialAttributes.tesla.deliveryState), 'MAP<STRING, STRING>') AS deliver_state_map, -- not needed in prod table
  struct (
    map_keys(deliver_state_map)[0] as delivery_state, --delivered or nonDelivered
    specialAttributes.tesla.deliveryState.delivered.vehicleType as vehicle_sale_type
  )  as tesla_special_attribute_struct,


  userOptedOut as has_user_opted_out,

  -- change to struct array
  legalTerms as legal_terms_struct_array, -- convert struct elements to curation naming standard as given below
  
  /*struct(
    legalTerms.appVersion as app_version,
    legalTerms.countryCode as country_code,
    legalTerms.consentType as consent_type,
    legalTerms.language as language,
    legalTerms.version as version,
    legalTerms.platform as device_platform,
    legalTerms.acceptedAt as accepted_est_timestamp
  ) as legal_terms_struct,*/

  struct (from_utc_timestamp(createdAt,'GMT-5') as create_est_timestamp, from_utc_timestamp(lastupdatedAt,'GMT-5') as update_est_timestamp) as source_timestamp_struct, 

  valid_from_est_timestamp,
  case when isDeleted = 'Y'  then valid_from_est_timestamp
       else coalesce(lead(valid_from_est_timestamp) OVER (PARTITION BY vehicleId ORDER BY valid_from_est_timestamp), cast('9999-12-31' as timestamp))
  end as valid_to_est_timestamp,

  case when isDeleted = 'N' and valid_to_est_timestamp = '9999-12-31 00:00:00.000' then true else false end as is_current,

  -- follow naming stds for the fields in this struct  
  requestContext as request_context_struct,

  isDeleted  -- not needed in prod table

from --identify new records or the records with chnages from prior update
(
  select vehicleId, partner, countryOfSale, activationDate, externalIds, vehicleAttributes, specialAttributes, userOptedOut, vehicleSource,  legalTerms, createdAt, lastupdatedAt, requestContext, isDeleted, valid_from_est_timestamp,

  case when lag(struct(vehicleId, partner, countryOfSale, activationDate, externalIds, vehicleAttributes, specialAttributes, userOptedOut, vehicleSource,  legalTerms, createdAt, lastupdatedAt,isDeleted)) 
      over(partition by vehicleId ORDER BY valid_from_est_timestamp)
      = struct(vehicleId, partner, countryOfSale, activationDate, externalIds, vehicleAttributes, specialAttributes, userOptedOut, vehicleSource,  legalTerms, createdAt, lastupdatedAt,isDeleted) 
  then false 
  else true 
  end as is_new_change

  from 
  ( --cleansed events deduping multiple updates with same timestamp
    select * from 
    (
      select vehicleId, partner, countryOfSale, activationDate, externalIds, vehicleAttributes, specialAttributes, userOptedOut, source as vehicleSource,  legalTerms, 
      createdAt, lastupdatedAt, requestContext, 'N' as isDeleted, from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp
      from cleansed_prod.user_services.vehicle_created_events
      where isTest = false
      union 
      select vehicleId, partner, countryOfSale, activationDate, externalIds, vehicleAttributes, specialAttributes, userOptedOut, source as vehicleSource,  legalTerms, 
      createdAt, lastupdatedAt, requestContext, 'N' as isDeleted, from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp
      from cleansed_prod.user_services.vehicle_updated_events
      where isTest = false
      union
      select vehicleId, partner, countryOfSale, activationDate, externalIds, vehicleAttributes, specialAttributes, userOptedOut, source as vehicleSource,  legalTerms, 
      createdAt, lastupdatedAt, requestContext, 'Y' as isDeleted, from_utc_timestamp(envelopeTime,'GMT-5') as valid_from_est_timestamp
      from cleansed_prod.user_services.vehicle_deleted_events
      where isTest = false
    )
    qualify row_number() over (PARTITION BY vehicleId  ORDER BY valid_from_est_timestamp, isDeleted desc) = 1
  ) 
) 
where is_new_change = true or isDeleted = 'Y' 
 ;

select * from sandbox_prod.sbosco.vehicle_scd order by vehicle_id, valid_from_est_timestamp;
