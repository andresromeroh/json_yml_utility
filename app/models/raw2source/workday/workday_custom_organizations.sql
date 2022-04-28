{{
    config(
            materialized="incremental",
            unique_key = 'custom_organization_key',
            tags=['WORKDAY_RAW2SOURCE']
    )
}}

WITH source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_custom_organizations') }} AS x

    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:Type::STRING AS custom_organization_type
        , raw_json:Subtype::STRING AS custom_organization_subtype
        , raw_json:Code::STRING AS custom_organization_code
        , raw_json:Last_Updated_Date::DATE AS last_updated_date
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective
        , raw_json:Reference_ID::STRING AS reference_id
        , raw_json:Inactive::BOOLEAN AS inactive
        , raw_json:Inactive_Date::DATE AS inactive_date
        , raw_json:Name::STRING AS custom_organization_name
        , raw_json:Superior_Organization_ID::STRING AS superior_organization_id

    FROM source_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, organization_hash AS (
    SELECT
        workday_id
        , custom_organization_type
        , custom_organization_subtype
        , custom_organization_code
        , last_updated_date
        , timestamp_effective
        , reference_id
        , custom_organization_name
        , superior_organization_id
        , inactive AS is_deleted
        , inactive_date
        , HASH(workday_id
            , custom_organization_type
            , custom_organization_subtype
            , custom_organization_code
            , last_updated_date
            , reference_id
            , is_deleted
            , inactive_date
            , custom_organization_name
            , superior_organization_id
        ) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , organization_last_record AS (
        SELECT
            workday_id
            , custom_organization_type
            , custom_organization_subtype
            , custom_organization_code
            , last_updated_date
            , reference_id
            , custom_organization_name
            , superior_organization_id
            , is_deleted
            , inactive_date
            , timestamp_effective
            , row_hash
        FROM {{ this }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id ORDER BY timestamp_effective DESC) = 1
    )
{% endif %}

, final_new_records AS (
    SELECT
        workday_id
        , custom_organization_type
        , custom_organization_subtype
        , custom_organization_code
        , last_updated_date
        , reference_id
        , custom_organization_name
        , superior_organization_id
        , is_deleted
        , inactive_date
        , timestamp_effective
        , row_hash
    FROM organization_hash

    {% if is_incremental() %}
        UNION
        SELECT
            workday_id
            , custom_organization_type
            , custom_organization_subtype
            , custom_organization_code
            , last_updated_date
            , reference_id
            , custom_organization_name
            , superior_organization_id
            , is_deleted
            , inactive_date
            , timestamp_effective
            , row_hash
        FROM organization_last_record
        WHERE NOT EXISTS (SELECT 1 FROM organization_hash
            WHERE organization_last_record.workday_id = organization_hash.workday_id
                AND organization_last_record.timestamp_effective = organization_hash.timestamp_effective)
    {% endif %}
)

, final AS (
    SELECT
        workday_id
        , custom_organization_type
        , custom_organization_subtype
        , custom_organization_code
        , last_updated_date
        , reference_id
        , custom_organization_name
        , superior_organization_id
        , is_deleted
        , inactive_date
        , timestamp_effective
        , row_hash
        , CASE WHEN LAG(row_hash) OVER(PARTITION BY workday_id ORDER BY timestamp_effective)
            != row_hash
            OR LAG(row_hash) OVER(PARTITION BY workday_id ORDER BY timestamp_effective) IS NULL
            THEN 1
        END AS row_change_filter
    FROM final_new_records
    QUALIFY row_change_filter = 1
)

SELECT
    {{ dbt_utils.surrogate_key([ "Workday_ID", "timestamp_effective"]) }} AS custom_organization_key
    , workday_id
    , custom_organization_code
    , custom_organization_name
    , custom_organization_type
    , custom_organization_subtype
    , superior_organization_id
    , last_updated_date
    , reference_id
    , is_deleted
    , inactive_date
    , timestamp_effective
    , row_hash
    , CURRENT_TIMESTAMP() AS etl_updated_timestamp
FROM final
