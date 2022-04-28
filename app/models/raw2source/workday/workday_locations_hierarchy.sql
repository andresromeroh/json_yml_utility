{{
  config(
    materialized = "incremental",
    unique_key='location_hierarchy_key',
    tags=['WORKDAY_RAW2SOURCE']
  )
}}

WITH source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_locations_hierarchy') }} AS x
    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:Name::STRING AS "name"
        , raw_json:Superior_Organization::STRING AS superior_organization
        , raw_json:Level_01_from_the_Top::STRING AS level_01_from_the_top
        , raw_json:Level_02_from_the_Top::STRING AS level_02_from_the_top
        , raw_json:Level_03_from_the_Top::STRING AS level_03_from_the_top
        , raw_json:Level_04_from_the_Top::STRING AS level_04_from_the_top
        , raw_json:Level_05_from_the_Top::STRING AS level_05_from_the_top
        , raw_json:Level_06_from_the_Top::STRING AS level_06_from_the_top
        , raw_json:Level_07_from_the_Top::STRING AS level_07_from_the_top
        , raw_json:Time_Zone::STRING AS time_zone
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective
    FROM source_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, locations_hierarchy_hash AS (
    SELECT
        workday_id
        , name
        , superior_organization
        , level_01_from_the_top
        , level_02_from_the_top
        , level_03_from_the_top
        , level_04_from_the_top
        , level_05_from_the_top
        , level_06_from_the_top
        , level_07_from_the_top
        , time_zone
        , timestamp_effective
        , HASH(workday_id
            , name
            , superior_organization
            , level_01_from_the_top
            , level_02_from_the_top
            , level_03_from_the_top
            , level_04_from_the_top
            , level_05_from_the_top
            , level_06_from_the_top
            , level_07_from_the_top
            , time_zone) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , locations_hierarchy_last_record AS (
        SELECT
            workday_id
            , name
            , superior_organization
            , level_01_from_the_top
            , level_02_from_the_top
            , level_03_from_the_top
            , level_04_from_the_top
            , level_05_from_the_top
            , level_06_from_the_top
            , level_07_from_the_top
            , time_zone
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
        , name
        , superior_organization
        , level_01_from_the_top
        , level_02_from_the_top
        , level_03_from_the_top
        , level_04_from_the_top
        , level_05_from_the_top
        , level_06_from_the_top
        , level_07_from_the_top
        , time_zone
        , timestamp_effective
        , row_hash
    FROM locations_hierarchy_hash

    {% if is_incremental() %}
        UNION
        SELECT
            workday_id
            , name
            , superior_organization
            , level_01_from_the_top
            , level_02_from_the_top
            , level_03_from_the_top
            , level_04_from_the_top
            , level_05_from_the_top
            , level_06_from_the_top
            , level_07_from_the_top
            , time_zone
            , timestamp_effective
            , row_hash
        FROM locations_hierarchy_last_record
        WHERE NOT EXISTS (SELECT 1 FROM locations_hierarchy_hash
            WHERE locations_hierarchy_last_record.workday_id = locations_hierarchy_hash.workday_id
                AND locations_hierarchy_last_record.timestamp_effective = locations_hierarchy_hash.timestamp_effective)
    {% endif %}
)

, final AS (
    SELECT
        workday_id
        , name
        , superior_organization
        , level_01_from_the_top
        , level_02_from_the_top
        , level_03_from_the_top
        , level_04_from_the_top
        , level_05_from_the_top
        , level_06_from_the_top
        , level_07_from_the_top
        , time_zone
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
    {{ dbt_utils.surrogate_key(["Workday_ID", "timestamp_effective"]) }} AS location_hierarchy_key
    , workday_id
    , name
    , superior_organization
    , level_01_from_the_top
    , level_02_from_the_top
    , level_03_from_the_top
    , level_04_from_the_top
    , level_05_from_the_top
    , level_06_from_the_top
    , level_07_from_the_top
    , time_zone
    , timestamp_effective
    , row_hash
    , CURRENT_TIMESTAMP() AS etl_updated_timestamp
FROM final
