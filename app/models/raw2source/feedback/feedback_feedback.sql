{{
    config(
            materialized="incremental",
            unique_key = 'feedback_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


WITH source_data AS (
    SELECT
        x.id
        , x.created
        , x.last_updated
        , x.form_name
        , x.feedback_type
        , x.level_id
        , x.reviewee_id
        , x.reviewer_id
        , x.round_id
        , x.rubric_id
        , x.created_duration
        , x.edit_locked
        , x.share_with_reviewee
        , x.role_id
        , x.reveal
        , x.saved_by_user
        , x.attributed
        , x.show_ratings
        , x.is_drafted
        , x.is_archived
        , x.feedback_source
        , x.next_role_id
        , x.custom_form_id
        , x.employee_ptl_info_id
        , x._h2_sf_inserted
        , x._h2_is_deleted
        , x._h2_binlog_ts_ms
    FROM {{ idw_package.dynamic_source('feedback_raw', 'feedback_feedback_history') }} AS x
    {% if is_incremental() %}
        WHERE x._h2_sf_inserted >= (SELECT MAX(s.etl_updated_timestamp)::DATE FROM {{ this }} AS s)
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY x.id, x._h2_binlog_ts_ms ORDER BY x._h2_binlog_ts_ms ASC) = 1
)

, dimension_history_hash AS (
    SELECT
        x.id::INTEGER AS id
        , x.created::TIMESTAMP_TZ AS created
        , x.last_updated::TIMESTAMP_TZ AS last_updated
        , x.form_name::STRING AS form_name
        , x.feedback_type::INTEGER AS feedback_type
        , x.level_id::INTEGER AS level_id
        , x.reviewee_id::INTEGER AS reviewee_id
        , x.reviewer_id::INTEGER AS reviewer_id
        , x.round_id::INTEGER AS round_id
        , x.rubric_id::INTEGER AS rubric_id
        , x.created_duration::INTEGER AS created_duration
        , x.edit_locked::BOOLEAN AS edit_locked
        , x.share_with_reviewee::BOOLEAN AS share_with_reviewee
        , x.role_id::INTEGER AS role_id
        , x.reveal::BOOLEAN AS reveal
        , x.saved_by_user::BOOLEAN AS saved_by_user
        , x.attributed::BOOLEAN AS attributed
        , x.show_ratings::BOOLEAN AS show_ratings
        , x.is_drafted::BOOLEAN AS is_drafted
        , x.is_archived::BOOLEAN AS is_archived
        , x.feedback_source::INTEGER AS feedback_source
        , x.next_role_id::INTEGER AS next_role_id
        , x.custom_form_id::INTEGER AS custom_form_id
        , x.employee_ptl_info_id::INTEGER AS employee_ptl_info_id
        , x._h2_sf_inserted::TIMESTAMP_TZ AS etl_updated_timestamp
        , x._h2_is_deleted::BOOLEAN AS is_deleted_in_source
        , x._h2_binlog_ts_ms::TIMESTAMP_TZ AS timestamp_effective
        , HASH(
            id
            , created
            , last_updated
            , form_name
            , feedback_type
            , level_id
            , reviewee_id
            , reviewer_id
            , round_id
            , rubric_id
            , created_duration
            , edit_locked
            , share_with_reviewee
            , role_id
            , reveal
            , saved_by_user
            , attributed
            , show_ratings
            , is_drafted
            , is_archived
            , feedback_source
            , next_role_id
            , custom_form_id
            , employee_ptl_info_id
        ) AS row_hash
    FROM source_data AS x
)

{% if is_incremental() %}
    , dimension_history_last_record AS (
        SELECT
            x.id
            , x.created
            , x.last_updated
            , x.form_name
            , x.feedback_type
            , x.level_id
            , x.reviewee_id
            , x.reviewer_id
            , x.round_id
            , x.rubric_id
            , x.created_duration
            , x.edit_locked
            , x.share_with_reviewee
            , x.role_id
            , x.reveal
            , x.saved_by_user
            , x.attributed
            , x.show_ratings
            , x.is_drafted
            , x.is_archived
            , x.feedback_source
            , x.next_role_id
            , x.custom_form_id
            , x.employee_ptl_info_id
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
        , created
        , last_updated
        , form_name
        , feedback_type
        , level_id
        , reviewee_id
        , reviewer_id
        , round_id
        , rubric_id
        , created_duration
        , edit_locked
        , share_with_reviewee
        , role_id
        , reveal
        , saved_by_user
        , attributed
        , show_ratings
        , is_drafted
        , is_archived
        , feedback_source
        , next_role_id
        , custom_form_id
        , employee_ptl_info_id
        , etl_updated_timestamp
        , is_deleted_in_source
        , timestamp_effective
        , row_hash
    FROM dimension_history_hash
    {% if is_incremental() %}
        UNION
        SELECT
            id
            , created
            , last_updated
            , form_name
            , feedback_type
            , level_id
            , reviewee_id
            , reviewer_id
            , round_id
            , rubric_id
            , created_duration
            , edit_locked
            , share_with_reviewee
            , role_id
            , reveal
            , saved_by_user
            , attributed
            , show_ratings
            , is_drafted
            , is_archived
            , feedback_source
            , next_role_id
            , custom_form_id
            , employee_ptl_info_id
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
        , created
        , last_updated
        , form_name
        , feedback_type
        , level_id
        , reviewee_id
        , reviewer_id
        , round_id
        , rubric_id
        , created_duration
        , edit_locked
        , share_with_reviewee
        , role_id
        , reveal
        , saved_by_user
        , attributed
        , show_ratings
        , is_drafted
        , is_archived
        , feedback_source
        , next_role_id
        , custom_form_id
        , employee_ptl_info_id
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
    {{ dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} AS feedback_key
    , id
    , created
    , last_updated
    , form_name
    , feedback_type
    , level_id
    , reviewee_id
    , reviewer_id
    , round_id
    , rubric_id
    , created_duration
    , edit_locked
    , share_with_reviewee
    , role_id
    , reveal
    , saved_by_user
    , attributed
    , show_ratings
    , is_drafted
    , is_archived
    , feedback_source
    , next_role_id
    , custom_form_id
    , employee_ptl_info_id
    , etl_updated_timestamp
    , is_deleted_in_source
    , timestamp_effective
    , row_hash
FROM final
