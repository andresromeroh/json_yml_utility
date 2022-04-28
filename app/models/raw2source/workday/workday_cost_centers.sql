{{
  config(
    materialized = "incremental",
    unique_key='cost_center_key',
    tags=['WORKDAY_RAW2SOURCE']
  )
}}

WITH source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_cost_centers') }} AS x
    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:Name::STRING AS cost_center_name
        , raw_json:Reference_ID::STRING AS reference_id
        , raw_json:Code::STRING AS cost_center_code
        , raw_json:Included_by_Organizations_group::VARIANT AS included_by_organizations_group
        , raw_json:Inactive::BOOLEAN AS inactive
        , raw_json:Inactive_Date::DATE AS inactive_date
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective
        , raw_json:Created_Moment::DATE AS created_date

    FROM source_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, cost_center_hash AS (
    SELECT
        workday_id
        , cost_center_name
        , reference_id
        , cost_center_code
        , included_by_organizations_group
        , inactive AS is_deleted
        , inactive_date
        , timestamp_effective
        , created_date
        , HASH(workday_id
            , cost_center_name
            , reference_id
            , cost_center_code
            , included_by_organizations_group
            , is_deleted
            , inactive_date
            , created_date) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , cost_center_last_record AS (
        SELECT
            workday_id
            , cost_center_name
            , reference_id
            , cost_center_code
            , included_by_organizations_group
            , is_deleted
            , inactive_date
            , created_date
            , timestamp_effective
            , row_hash
            , ROW_NUMBER() OVER (PARTITION BY workday_id ORDER BY timestamp_effective DESC) AS rn
        FROM {{ this }}
        QUALIFY rn = 1
    )
{% endif %}

, final_new_records AS (
    SELECT
        workday_id
        , cost_center_name
        , reference_id
        , cost_center_code
        , included_by_organizations_group
        , is_deleted
        , inactive_date
        , created_date
        , timestamp_effective
        , row_hash
    FROM cost_center_hash

    {% if is_incremental() %}
        UNION
        SELECT
            workday_id
            , cost_center_name
            , reference_id
            , cost_center_code
            , included_by_organizations_group
            , is_deleted
            , inactive_date
            , created_date
            , timestamp_effective
            , row_hash
        FROM cost_center_last_record
        WHERE NOT EXISTS (SELECT 1 FROM cost_center_hash
            WHERE cost_center_last_record.workday_id = cost_center_hash.workday_id
                AND cost_center_last_record.timestamp_effective = cost_center_hash.timestamp_effective)
    {% endif %}
)


, final AS (
    SELECT
        workday_id
        , cost_center_name
        , reference_id
        , cost_center_code
        , included_by_organizations_group
        , is_deleted
        , inactive_date
        , created_date
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
    {{ dbt_utils.surrogate_key(["Workday_ID", "timestamp_effective"]) }} AS cost_center_key
    , workday_id
    , cost_center_name
    , reference_id
    , cost_center_code
    , included_by_organizations_group
    , is_deleted
    , inactive_date
    , created_date
    , timestamp_effective
    , row_hash
    , CURRENT_TIMESTAMP() AS etl_updated_timestamp
FROM final
