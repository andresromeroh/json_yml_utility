{{
    config(
            materialized="incremental",
            unique_key = 'company_key',
            tags=['WORKDAY_RAW2SOURCE']
    )
}}

WITH
source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_companies') }} AS x
    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:Last_Updated_Date::DATE AS last_updated_date
        , raw_json:Created_Moment::DATE AS created_date
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective
        , raw_json:Reference_ID::STRING AS reference_id
        , raw_json:Inactive::BOOLEAN AS inactive
        , raw_json:Code::STRING AS company_code
        , raw_json:Name::STRING AS company_name

    FROM source_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, company_hash AS (
    SELECT
        workday_id
        , last_updated_date
        , created_date
        , timestamp_effective
        , reference_id
        , inactive AS is_deleted
        , company_code
        , company_name
        , HASH(workday_id
            , last_updated_date
            , created_date
            , reference_id
            , is_deleted
            , company_code
            , company_name
        ) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , company_last_record AS (
        SELECT
            workday_id
            , last_updated_date
            , created_date
            , reference_id
            , is_deleted
            , company_code
            , company_name
            , timestamp_effective
            , row_hash
        FROM {{ this }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id ORDER BY timestamp_effective DESC) = 1
    )
{% endif %}

, final_new_records AS (
    SELECT
        workday_id
        , last_updated_date
        , created_date
        , reference_id
        , is_deleted
        , company_code
        , company_name
        , timestamp_effective
        , row_hash
    FROM company_hash

    {% if is_incremental() %}
        UNION
        SELECT
            workday_id
            , last_updated_date
            , created_date
            , reference_id
            , is_deleted
            , company_code
            , company_name
            , timestamp_effective
            , row_hash
        FROM company_last_record
        WHERE NOT EXISTS (SELECT 1 FROM company_hash
            WHERE company_last_record.workday_id = company_hash.workday_id
                AND company_last_record.timestamp_effective = company_hash.timestamp_effective)
    {% endif %}
)

, final AS (
    SELECT
        workday_id
        , last_updated_date
        , created_date
        , reference_id
        , is_deleted
        , company_code
        , company_name
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
    {{ dbt_utils.surrogate_key([ "Workday_ID", "timestamp_effective"]) }} AS company_key
    , workday_id
    , last_updated_date
    , created_date
    , reference_id
    , is_deleted
    , company_code
    , company_name
    , timestamp_effective
    , row_hash
    , CURRENT_TIMESTAMP() AS etl_updated_timestamp
FROM final
