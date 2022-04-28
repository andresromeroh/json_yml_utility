{{
    config(
            materialized="incremental",
            unique_key = 'workday_profile_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


WITH source_data AS (
    SELECT
        x.id
        , x.active
        , x.first_name
        , x.last_name
        , x.preferred_first_name
        , x.ldap
        , x.employee_id
        , x.email
        , x.hire_date
        , x.job_profile
        , x.job_profile_start_date
        , x.job_family
        , x.job_family_group
        , x.supervisory_organization_id
        , x.supervisory_organization_name
        , x.business_unit
        , x.division
        , x."GROUP"
        , x.cost_center_id
        , x.cost_center_name
        , x.manager_ldap
        , x.manager_email
        , x.manager_id
        , x.user_id
        , x.manager_employee_id
        , x.current_office
        , x.home_office
        , x._h2_sf_inserted
        , x._h2_is_deleted
        , x._h2_binlog_ts_ms
    FROM {{ idw_package.dynamic_source('feedback_raw', 'indeedworkday_workdayprofile_history') }} AS x
    {% if is_incremental() %}
        WHERE x._h2_sf_inserted >= (SELECT MAX(s.etl_updated_timestamp)::DATE FROM {{ this }} AS s)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY x.id, x._h2_binlog_ts_ms ORDER BY x._h2_binlog_ts_ms ASC) = 1
)

, dimension_history_hash AS (
    SELECT
        x.id::INTEGER AS id
        , x.active::BOOLEAN AS active
        , x.first_name::STRING AS first_name
        , x.last_name::STRING AS last_name
        , x.preferred_first_name::STRING AS preferred_first_name
        , x.ldap::STRING AS ldap
        , x.employee_id::STRING AS employee_id
        , x.email::STRING AS email
        , x.hire_date::TIMESTAMP_TZ AS hire_date
        , x.job_profile::STRING AS job_profile
        , x.job_profile_start_date::TIMESTAMP_TZ AS job_profile_start_date
        , x.job_family::STRING AS job_family
        , x.job_family_group::STRING AS job_family_group
        , x.supervisory_organization_id::STRING AS supervisory_organization_id
        , x.supervisory_organization_name::STRING AS supervisory_organization_name
        , x.business_unit::STRING AS business_unit
        , x.division::STRING AS division
        , x."GROUP"::STRING AS "GROUP"
        , x.cost_center_id::INTEGER AS cost_center_id
        , x.cost_center_name::STRING AS cost_center_name
        , x.manager_ldap::STRING AS manager_ldap
        , x.manager_email::STRING AS manager_email
        , x.manager_id::INTEGER AS manager_id
        , x.user_id::INTEGER AS "USER_ID"
        , x.manager_employee_id::STRING AS manager_employee_id
        , x.current_office::STRING AS current_office
        , x.home_office::STRING AS home_office
        , x._h2_sf_inserted::TIMESTAMP_TZ AS etl_updated_timestamp
        , x._h2_is_deleted::BOOLEAN AS is_deleted_in_source
        , x._h2_binlog_ts_ms::TIMESTAMP_TZ AS timestamp_effective
        , HASH(
            id
            , active
            , first_name
            , last_name
            , preferred_first_name
            , ldap
            , employee_id
            , email
            , hire_date
            , job_profile
            , job_profile_start_date
            , job_family
            , job_family_group
            , supervisory_organization_id
            , supervisory_organization_name
            , business_unit
            , division
            , "GROUP"
            , cost_center_id
            , cost_center_name
            , manager_ldap
            , manager_email
            , manager_id
            , "USER_ID"
            , manager_employee_id
            , current_office
            , home_office

        ) AS row_hash
    FROM source_data AS x
)

{% if is_incremental() %}
    , dimension_history_last_record AS (
        SELECT
            x.id
            , x.active
            , x.first_name
            , x.last_name
            , x.preferred_first_name
            , x.ldap
            , x.employee_id
            , x.email
            , x.hire_date
            , x.job_profile
            , x.job_profile_start_date
            , x.job_family
            , x.job_family_group
            , x.supervisory_organization_id
            , x.supervisory_organization_name
            , x.business_unit
            , x.division
            , x."GROUP"
            , x.cost_center_id
            , x.cost_center_name
            , x.manager_ldap
            , x.manager_email
            , x.manager_id
            , x.user_id
            , x.manager_employee_id
            , x.current_office
            , x.home_office
            , x.etl_updated_timestamp
            , x.is_deleted_in_source
            , x.timestamp_effective
            , x.row_hash
        FROM {{ this }} AS x
        QUALIFY ROW_NUMBER() OVER (PARTITION BY x.id ORDER BY x.timestamp_effective DESC) = 1
    )
{% endif %}

, final_new_records AS (
    SELECT
        id
        , active
        , first_name
        , last_name
        , preferred_first_name
        , ldap
        , employee_id
        , email
        , hire_date
        , job_profile
        , job_profile_start_date
        , job_family
        , job_family_group
        , supervisory_organization_id
        , supervisory_organization_name
        , business_unit
        , division
        , "GROUP"
        , cost_center_id
        , cost_center_name
        , manager_ldap
        , manager_email
        , manager_id
        , "USER_ID"
        , manager_employee_id
        , current_office
        , home_office
        , etl_updated_timestamp
        , is_deleted_in_source
        , timestamp_effective
        , row_hash
    FROM dimension_history_hash
    {% if is_incremental() %}
        UNION
        SELECT
            id
            , active
            , first_name
            , last_name
            , preferred_first_name
            , ldap
            , employee_id
            , email
            , hire_date
            , job_profile
            , job_profile_start_date
            , job_family
            , job_family_group
            , supervisory_organization_id
            , supervisory_organization_name
            , business_unit
            , division
            , "GROUP"
            , cost_center_id
            , cost_center_name
            , manager_ldap
            , manager_email
            , manager_id
            , "USER_ID"
            , manager_employee_id
            , current_office
            , home_office
            , etl_updated_timestamp
            , is_deleted_in_source
            , timestamp_effective
            , row_hash
        FROM dimension_history_last_record AS l
        WHERE NOT EXISTS (
                SELECT 1 FROM dimension_history_hash AS h
                WHERE l.id = h.id
                    AND l.timestamp_effective = h.timestamp_effective
        )
    {% endif %}
)

, final AS (
    SELECT
        id
        , active
        , first_name
        , last_name
        , preferred_first_name
        , ldap
        , employee_id
        , email
        , hire_date
        , job_profile
        , job_profile_start_date
        , job_family
        , job_family_group
        , supervisory_organization_id
        , supervisory_organization_name
        , business_unit
        , division
        , "GROUP"
        , cost_center_id
        , cost_center_name
        , manager_ldap
        , manager_email
        , manager_id
        , "USER_ID"
        , manager_employee_id
        , current_office
        , home_office
        , etl_updated_timestamp
        , is_deleted_in_source
        , timestamp_effective
        , row_hash
        , CASE WHEN LAG(row_hash) OVER(PARTITION BY id ORDER BY timestamp_effective)
            != row_hash
            OR LAG(row_hash) OVER(PARTITION BY id ORDER BY timestamp_effective) IS NULL
            THEN 1
        END AS row_change_filter
    FROM final_new_records
    QUALIFY row_change_filter = 1
)

SELECT
    {{ dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} AS workday_profile_key
    , id
    , active
    , first_name
    , last_name
    , preferred_first_name
    , ldap
    , employee_id
    , email
    , hire_date
    , job_profile
    , job_profile_start_date
    , job_family
    , job_family_group
    , supervisory_organization_id
    , supervisory_organization_name
    , business_unit
    , division
    , "GROUP"
    , cost_center_id
    , cost_center_name
    , manager_ldap
    , manager_email
    , manager_id
    , "USER_ID"
    , manager_employee_id
    , current_office
    , home_office
    , etl_updated_timestamp
    , is_deleted_in_source
    , timestamp_effective
    , row_hash
FROM final
