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
FROM {{ idw_package.dynamic_source('icims_raw', 'company') }} d
, LATERAL FLATTEN (input => d.raw_data:fields::VARIANT) as flatten_fields

{% if is_incremental() %}
WHERE raw_inserted_timestamp >= (SELECT max(etl_updated_timestamp) FROM {{ this }})
{% endif %}

)
, source_data as (
    SELECT
      deleted
    , id
    , lastUpdated, max(case when flatten_name = 'name' then flatten_value::STRING end) AS name
    , max(case when flatten_name = 'externalid' then flatten_value::STRING end) AS externalid
    , max(case when flatten_name = 'field61991' then flatten_value:defaultAttributes:Label::STRING end) AS field61991
    , max(case when flatten_name = 'field30416' then flatten_value::STRING end) AS field30416
    , max(case when flatten_name = 'field30415' then flatten_value::STRING end) AS field30415
    , max(case when flatten_name = 'field62851' then flatten_value::STRING end) AS field62851
    , max(case when flatten_name = 'field27655' then flatten_value:defaultAttributes:Label::STRING end) AS field27655
    , max(case when flatten_name = 'field30414' then flatten_value::STRING end) AS field30414
    , max(case when flatten_name = 'field30410' then flatten_value::STRING end) AS field30410
    , max(case when flatten_name = 'field30412' then flatten_value::STRING end) AS field30412
    , max(case when flatten_name = 'field30413' then flatten_value::STRING end) AS field30413
    , max(case when flatten_name = 'synopsis' then flatten_value::STRING end) AS synopsis
    , max(case when flatten_name = 'i9classification' then flatten_value:defaultAttributes:Label::STRING end) AS i9classification
    , max(case when flatten_name = 'field62979' then flatten_value::STRING end) AS field62979
    , max(case when flatten_name = 'field62978' then flatten_value::STRING end) AS field62978
    , max(case when flatten_name = 'field68748' then flatten_value::STRING end) AS field68748
    , max(case when flatten_name = 'field30411' then flatten_value::STRING end) AS field30411
    , max(case when flatten_name = 'field63187' then flatten_value::STRING end) AS field63187
    , max(case when flatten_name = 'field58047' then flatten_value::STRING end) AS field58047
    , max(case when flatten_name = 'quicksearch' then flatten_value::STRING end) AS quicksearch
    , max(case when flatten_name = 'createdby' then flatten_value:id::INTEGER end) AS createdby
    , max(case when flatten_name = 'createddate' then flatten_value::TIMESTAMP end) AS createddate
    , max(case when flatten_name = 'updatedby' then flatten_value:id::INTEGER end) AS updatedby
    , max(case when flatten_name = 'field30426' then flatten_value:id::INTEGER end) AS field30426
    , max(case when flatten_name = 'fulltext' then flatten_value::STRING end) AS fulltext
    , max(case when flatten_name = 'folder' then flatten_value:defaultAttributes:Label::STRING end) AS folder
    , max(case when flatten_name = 'updateddate' then flatten_value::TIMESTAMP end) AS updateddate
    , max(case when flatten_name = 'field30389' then flatten_value::STRING end) AS field30389
    , max(case when flatten_name = 'addresses' then flatten_value::STRING end) AS addresses
    , max(case when flatten_name = 'field27846' then flatten_value:defaultAttributes:Label::STRING end) AS field27846
    , max(case when flatten_name = 'field68934' then flatten_value::STRING end) AS field68934
    , max(case when flatten_name = 'field61990' then flatten_value:defaultAttributes:Label::STRING end) AS field61990
    , max(case when flatten_name = 'field27823' then flatten_value::STRING end) AS field27823
    , max(case when flatten_name = 'field61992' then flatten_value:defaultAttributes:Label::STRING end) AS field61992
    , max(case when flatten_name = 'field63938' then flatten_value::STRING end) AS field63938
    , max(case when flatten_name = 'field30425' then flatten_value:id::INTEGER end) AS field30425
    , max(case when flatten_name = 'field63937' then flatten_value::STRING end) AS field63937
    , max(case when flatten_name = 'field27845' then flatten_value:defaultAttributes:Label::STRING end) AS field27845
    , max(case when flatten_name = 'field68749' then flatten_value::STRING end) AS field68749
    , max(case when flatten_name = 'field30417' then flatten_value::STRING end) AS field30417
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