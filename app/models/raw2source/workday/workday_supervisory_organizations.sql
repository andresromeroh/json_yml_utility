{{
  config(
    materialized = "incremental",
    unique_key='supervisory_organization_key',
    tags=['WORKDAY_RAW2SOURCE']
  )
}}


WITH source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_supervisory_organizations') }} AS x
    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:Name::STRING AS "name"
        , raw_json:Reference_ID::STRING AS reference_id
        , raw_json:Manager_ID::STRING AS manager_id
        , raw_json:Superior_Organization::STRING AS superior_organization
        , raw_json:Inactive::BOOLEAN AS inactive
        , raw_json:Inactive_date::DATE AS inactive_date
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective

    FROM source_data
    WHERE timestamp_effective IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, supervisory_organization_hash AS (
    SELECT
        workday_id
        , name
        , reference_id
        , manager_id
        , superior_organization
        , inactive AS is_deleted
        , inactive_date
        , timestamp_effective
        , HASH(workday_id
            , name
            , reference_id
            , manager_id
            , superior_organization
            , is_deleted
            , inactive_date) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , supervisory_organization_last_record AS (
        SELECT
            workday_id
            , name
            , reference_id
            , manager_id
            , superior_organization
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
        , name
        , reference_id
        , manager_id
        , superior_organization
        , is_deleted
        , inactive_date
        , timestamp_effective
        , row_hash
    FROM supervisory_organization_hash

    {% if is_incremental() %}
        UNION
        SELECT
            workday_id
            , name
            , reference_id
            , manager_id
            , superior_organization
            , is_deleted
            , inactive_date
            , timestamp_effective
            , row_hash
        FROM supervisory_organization_last_record
        WHERE NOT EXISTS (SELECT 1 FROM supervisory_organization_hash
            WHERE supervisory_organization_last_record.workday_id = supervisory_organization_hash.workday_id
                AND supervisory_organization_last_record.timestamp_effective = supervisory_organization_hash.timestamp_effective)
    {% endif %}
)

, final AS (
    SELECT
        workday_id
        , name
        , reference_id
        , manager_id
        , superior_organization
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
    {{ dbt_utils.surrogate_key(["Workday_ID", "timestamp_effective"]) }} AS supervisory_organization_key
    , workday_id
    , name
    , reference_id
    , manager_id
    , superior_organization
    , is_deleted
    , inactive_date
    , timestamp_effective
    , row_hash
    , CURRENT_TIMESTAMP() AS etl_updated_timestamp
FROM final
