{{
    config(
            materialized="incremental",
            unique_key = 'location_key',
            tags=['WORKDAY_RAW2SOURCE']
    )
}}

WITH
source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_locations') }} AS x
    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:City::STRING AS city
        , raw_json:State_Code::STRING AS state_code
        , raw_json:State_Name::STRING AS state_name
        , raw_json:Country_Code::STRING AS country_code
        , raw_json:Country_Name::STRING AS country_name
        , raw_json:Region::STRING AS region
        , raw_json:Location_Name::STRING AS location_name
        , raw_json:Location_Type::STRING AS location_type
        , raw_json:Location_Usage::STRING AS location_usage
        , raw_json:Reference_ID::STRING AS reference_id
        , raw_json:Time_Zone::STRING AS time_zone
        , raw_json:Inactive::BOOLEAN AS inactive
        , raw_json:Inactive_Date::DATE AS inactive_date
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective
        , raw_json:Location_Hierarchies_group::STRING AS location_hierarchies_group
        , raw_json:Superior_Location::STRING AS superior_location

    FROM source_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, location_hash AS (
    SELECT
        workday_id
        , city
        , state_code
        , state_name
        , country_code
        , country_name
        , region
        , location_name
        , location_type
        , location_usage
        , reference_id
        , time_zone
        , inactive AS is_deleted
        , inactive_date
        , location_hierarchies_group
        , superior_location
        , timestamp_effective
        , HASH(
            workday_id
            , city
            , state_code
            , state_name
            , country_code
            , country_name
            , region
            , location_name
            , location_type
            , location_usage
            , reference_id
            , time_zone
            , is_deleted
            , inactive_date
            , location_hierarchies_group
            , superior_location
        ) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , location_last_record AS (
        SELECT
            locations.workday_id
            , locations.city
            , locations.state_code
            , locations.state_name
            , locations.country_code
            , locations.country_name
            , locations.region
            , locations.location_name
            , locations.location_type
            , locations.location_usage
            , locations.reference_id
            , locations.time_zone
            , locations.is_deleted
            , locations.inactive_date
            , locations.location_hierarchies_group
            , locations.superior_location
            , locations.timestamp_effective
            , locations.row_hash
            , ROW_NUMBER(
            ) OVER (
                PARTITION BY locations.workday_id ORDER BY locations.timestamp_effective DESC) AS rn
        FROM {{ this }} AS locations
        QUALIFY rn = 1
    )
{% endif %}
, final_new_records AS (
    SELECT
        workday_id
        , city
        , state_code
        , state_name
        , country_code
        , country_name
        , region
        , location_name
        , location_type
        , location_usage
        , reference_id
        , time_zone
        , is_deleted
        , inactive_date
        , location_hierarchies_group
        , superior_location
        , timestamp_effective
        , row_hash
    FROM location_hash

    {% if is_incremental() %}
        UNION
        SELECT
            workday_id
            , city
            , state_code
            , state_name
            , country_code
            , country_name
            , region
            , location_name
            , location_type
            , location_usage
            , reference_id
            , time_zone
            , is_deleted
            , inactive_date
            , location_hierarchies_group
            , superior_location
            , timestamp_effective
            , row_hash
        FROM location_last_record
        WHERE NOT EXISTS (SELECT 1 FROM location_hash
            WHERE location_last_record.workday_id = location_hash.workday_id
                AND location_last_record.timestamp_effective = location_hash.timestamp_effective)
    {% endif %}
)

, final AS (
    SELECT
        workday_id
        , city
        , state_code
        , state_name
        , country_code
        , country_name
        , region
        , location_name
        , location_type
        , location_usage
        , reference_id
        , time_zone
        , is_deleted
        , inactive_date
        , location_hierarchies_group
        , superior_location
        , timestamp_effective
        , row_hash
        , CASE WHEN LAG(
            row_hash) OVER (
            PARTITION BY workday_id ORDER BY timestamp_effective)
            != row_hash
            OR LAG(
                row_hash) OVER (
                PARTITION BY workday_id ORDER BY timestamp_effective) IS NULL
            THEN 1
        END AS row_change_filter
    FROM final_new_records
    QUALIFY row_change_filter = 1
)

SELECT
    {{ dbt_utils.surrogate_key([ "Workday_ID", "timestamp_effective"]) }} AS location_key
    , workday_id
    , city
    , state_code
    , state_name
    , country_code
    , country_name
    , region
    , location_name
    , location_type
    , location_usage
    , reference_id
    , time_zone
    , is_deleted
    , inactive_date
    , location_hierarchies_group
    , superior_location
    , timestamp_effective
    , row_hash
    , CURRENT_TIMESTAMP(
    ) AS etl_updated_timestamp
FROM final
