{{
  config(
    materialized = "table",
    tags=['icims_raw2source']
  )
}}
with source_flatten_data as
(SELECT DISTINCT
      d.raw_data:deleted::VARCHAR AS deleted
    , d.raw_data:id::INTEGER AS id
    , d.raw_data:lastUpdated::TIMESTAMP AS lastUpdated
    , flatten_fields.value:name flatten_name
    , flatten_fields.value:type flatten_type
    , flatten_fields.value:value flatten_value
FROM {{ idw_package.dynamic_source('icims_raw', 'connecteventworkflowsource') }} d
, LATERAL FLATTEN (input => d.raw_data:fields::VARIANT) as flatten_fields

{% if is_incremental() %}
WHERE raw_inserted_timestamp >= (SELECT max(etl_updated_timestamp) FROM {{ this }})
{% endif %}

)
, source_data as (
    SELECT
      deleted
    , id
    , lastUpdated, max(case when flatten_name = 'talentpool' then flatten_value:id::INTEGER end) AS talentpool
    , max(case when flatten_name = 'eventattendee' then flatten_value:id::INTEGER end) AS eventattendee
    , max(case when flatten_name = 'updatedby' then flatten_value:id::INTEGER end) AS updatedby
    , max(case when flatten_name = 'createddate' then flatten_value::TIMESTAMP end) AS createddate
    , max(case when flatten_name = 'updateddate' then flatten_value::TIMESTAMP end) AS updateddate
    , max(case when flatten_name = 'createdby' then flatten_value:id::INTEGER end) AS createdby
    from source_flatten_data
    group by deleted
    , id
    , lastUpdated
)
, final as
(SELECT d.*
, typeof(HASH(d.*)) as row_hash
, current_timestamp() as etl_updated_timestamp
 from source_data d)
SELECT src.* from final src

{% if is_incremental() %}
LEFT OUTER JOIN {{ this }} tgt
ON src.id = tgt.id and src.row_hash = tgt.row_hash
WHERE tgt.id IS NULL
{% endif %}