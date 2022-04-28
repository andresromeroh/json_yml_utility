{{
  config(
    materialized = "incremental",
    unique_key='job_profiles_key',
    tags=['WORKDAY_RAW2SOURCE']
  )
}}

WITH
source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_job_profiles') }} AS x
    {% if is_incremental() %}
        WHERE x.raw_inserted_timestamp
            >= (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

, raw_parsed AS (
    SELECT
        raw_json:Workday_ID::STRING AS workday_id
        , raw_json:Compensation_Grade_Hierarchy::STRING AS compensation_grade_hierarchy
        , raw_json:Job_Profile_Name::STRING AS job_profile_name
        , raw_json:Report_Effective_Date::DATE::TIMESTAMP_TZ AS timestamp_effective
        , raw_json:Compensation_Grade::STRING AS compensation_grade
        , raw_json:Reference_ID::STRING AS reference_id
        , raw_json:Inactive::BOOLEAN AS inactive
        , raw_json:Job_Profile_Exempt_Group::STRING AS job_profile_exempt_group
        , raw_json:Job_Profile_Pay_Rate_Group::STRING AS job_profile_pay_rate_group
        , raw_json:Job_Classifications_group::STRING AS job_classifications_group
        , raw_json:Job_Families_on_Job_Profile_group[0]:Job_Family::STRING AS job_family
        , raw_json:Job_Families_on_Job_Profile_group[0]:Job_Family_Group::STRING AS job_family_group
        , raw_json:Job_Families_on_Job_Profile_group::STRING AS job_families_on_job_profile_group

    FROM source_data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY workday_id, timestamp_effective ORDER BY timestamp_effective ASC) = 1
)

, job_profile_hash AS (
    SELECT
        workday_id
        , compensation_grade_hierarchy
        , job_profile_name
        , timestamp_effective
        , compensation_grade
        , reference_id
        , job_profile_exempt_group
        , job_profile_pay_rate_group
        , job_classifications_group
        , job_family
        , job_family_group
        , job_families_on_job_profile_group
        , inactive AS is_deleted
        , HASH(workday_id
            , compensation_grade_hierarchy
            , job_profile_name
            , compensation_grade
            , reference_id
            , job_profile_exempt_group
            , job_profile_pay_rate_group
            , job_classifications_group
            , job_family
            , job_family_group
            , job_families_on_job_profile_group
            , is_deleted) AS row_hash
    FROM raw_parsed
)

{% if is_incremental() %}
    , job_profile_last_record AS (
        SELECT
            workday_id
            , compensation_grade_hierarchy
            , job_profile_name
            , timestamp_effective
            , compensation_grade
            , reference_id
            , is_deleted
            , job_profile_exempt_group
            , job_profile_pay_rate_group
            , job_classifications_group
            , job_family
            , job_family_group
            , job_families_on_job_profile_group
            , row_hash
            , ROW_NUMBER() OVER (PARTITION BY workday_id ORDER BY timestamp_effective DESC) AS rn
        FROM {{ this }}
        QUALIFY rn = 1
    )
{% endif %}

, final_new_records as
(
    SELECT
        workday_id
        , compensation_grade_hierarchy
        , job_profile_name
        , timestamp_effective
        , compensation_grade
        , reference_id
        , job_profile_exempt_group
        , job_profile_pay_rate_group
        , job_classifications_group
        , job_family
        , job_family_group
        , job_families_on_job_profile_group
        , is_deleted
        , row_hash
    FROM job_profile_hash

{% if is_incremental() %}
    UNION
    SELECT
        workday_id
        , compensation_grade_hierarchy
        , job_profile_name
        , timestamp_effective
        , compensation_grade
        , reference_id
        , job_profile_exempt_group
        , job_profile_pay_rate_group
        , job_classifications_group
        , job_family
        , job_family_group
        , job_families_on_job_profile_group
        , is_deleted
        , row_hash
    FROM job_profile_last_record l
    WHERE NOT EXISTS (
        SELECT 1 FROM job_profile_hash h
        WHERE l.workday_ID = h.workday_ID
            AND l.timestamp_effective = h.timestamp_effective)
{% endif %}
)

, final as (
    SELECT   
        workday_id
        , compensation_grade_hierarchy
        , job_profile_name
        , timestamp_effective
        , compensation_grade
        , reference_id
        , job_profile_exempt_group
        , job_profile_pay_rate_group
        , job_classifications_group
        , job_family
        , job_family_group
        , job_families_on_job_profile_group
        , is_deleted
        , row_hash
        , CASE WHEN LAG(row_hash) OVER(PARTITION BY Workday_ID ORDER BY timestamp_effective)
            <> row_hash
            OR LAG(row_hash) OVER(PARTITION BY Workday_ID ORDER BY timestamp_effective) IS NULL
            THEN 1
        END AS row_change_filter
    FROM final_new_records
    QUALIFY row_change_filter = 1
)

SELECT {{dbt_utils.surrogate_key([ "Workday_ID", "timestamp_effective"]) }} AS job_profiles_key
    , workday_id
    , compensation_grade_hierarchy
    , job_profile_name
    , timestamp_effective
    , compensation_grade
    , reference_id
    , job_profile_exempt_group
    , job_profile_pay_rate_group
    , job_classifications_group
    , job_family
    , job_family_group
    , job_families_on_job_profile_group
    , is_deleted
    , row_hash
    , current_timestamp() as etl_updated_timestamp
FROM final
