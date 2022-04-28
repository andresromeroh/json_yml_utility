{{
    config(
            materialized="incremental",
            unique_key = 'organization_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


with source_data as(
        select
                x.ID,
                x.NAME,
                x.FEEDBACK_REQUEST_TEMPLATE_ID,
                x.FEEDBACK_REMINDER_TEMPLATE_ID,
                x.PEERFEEDBACK_FORM,
                x.REVIEW_FORM,
                x.SELFEVAL_FORM,
                x.UPWARD_FEEDBACK_RUBRIC,
                x.HIDE_ROLE,
                x.HIDE_OVERALL_RATING,
                x.HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
                x.UPWARD_FEEDBACK_RUBRIC_ID,
                x.REVEAL_RATING,
                x.ANYTIME_FEEDBACK_RUBRIC_ID,
                x.IS_PEER_FEEDBACK_ATTRIBUTED,
                x.SHOW_PEER_FEEDBACK_RATINGS,
                x.AUTO_SHARE_PEER_FEEDBACK,
                x.HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
                x.SHOW_AWARDS_PAGE,
                x.SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
                x.HIDE_PREVIOUS_QUARTER_REVIEW,
                x._H2_SF_INSERTED,
                x._H2_IS_DELETED,
                x._H2_BINLOG_TS_MS
        from {{ idw_package.dynamic_source('feedback_raw', 'ckapp_organization_history') }} x
        {% if is_incremental() %}
                where x._H2_SF_INSERTED >= (select max(s.ETL_UPDATED_TIMESTAMP)::date from {{ this }} as s )
        {% endif %}
        qualify row_number() over (partition by x.ID, x._H2_BINLOG_TS_MS order by x._H2_BINLOG_TS_MS asc) = 1
)

,organization_history_hash as (
        select
                x.ID::integer                                              as ID
                ,x.NAME::string                                            as "NAME"
                ,x.FEEDBACK_REQUEST_TEMPLATE_ID::integer                   as FEEDBACK_REQUEST_TEMPLATE_ID
                ,x.FEEDBACK_REMINDER_TEMPLATE_ID::integer                  as FEEDBACK_REMINDER_TEMPLATE_ID
                ,x.PEERFEEDBACK_FORM::string                               as PEERFEEDBACK_FORM
                ,x.REVIEW_FORM::string                                     as REVIEW_FORM
                ,x.SELFEVAL_FORM::string                                   as SELFEVAL_FORM
                ,x.UPWARD_FEEDBACK_RUBRIC::string                          as UPWARD_FEEDBACK_RUBRIC
                ,x.HIDE_ROLE::boolean                                      as HIDE_ROLE
                ,x.HIDE_OVERALL_RATING::boolean                            as HIDE_OVERALL_RATING
                ,x.HIDE_SELFEVAL_RATING_UNTIL_LOCKED::boolean              as HIDE_SELFEVAL_RATING_UNTIL_LOCKED
                ,x.UPWARD_FEEDBACK_RUBRIC_ID::integer                      as UPWARD_FEEDBACK_RUBRIC_ID
                ,x.REVEAL_RATING::boolean                                  as REVEAL_RATING
                ,x.ANYTIME_FEEDBACK_RUBRIC_ID::boolean                     as ANYTIME_FEEDBACK_RUBRIC_ID
                ,x.IS_PEER_FEEDBACK_ATTRIBUTED::boolean                    as IS_PEER_FEEDBACK_ATTRIBUTED
                ,x.SHOW_PEER_FEEDBACK_RATINGS::boolean                     as SHOW_PEER_FEEDBACK_RATINGS
                ,x.AUTO_SHARE_PEER_FEEDBACK::boolean                       as AUTO_SHARE_PEER_FEEDBACK
                ,x.HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW::boolean       as HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW
                ,x.SHOW_AWARDS_PAGE::boolean                               as SHOW_AWARDS_PAGE
                ,x.SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION::boolean           as SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION
                ,x.HIDE_PREVIOUS_QUARTER_REVIEW::boolean                   as HIDE_PREVIOUS_QUARTER_REVIEW
                ,x._H2_SF_INSERTED::timestamp_tz                           as ETL_UPDATED_TIMESTAMP
                ,x._H2_IS_DELETED::boolean                                 as IS_DELETED_IN_SOURCE
                ,x._H2_BINLOG_TS_MS::timestamp_tz                          as TIMESTAMP_EFFECTIVE
                ,hash(
                    ID,
                    "NAME",
                    FEEDBACK_REQUEST_TEMPLATE_ID,
                    FEEDBACK_REMINDER_TEMPLATE_ID,
                    PEERFEEDBACK_FORM,
                    REVIEW_FORM,
                    SELFEVAL_FORM,
                    UPWARD_FEEDBACK_RUBRIC,
                    HIDE_ROLE,
                    HIDE_OVERALL_RATING,
                    HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
                    UPWARD_FEEDBACK_RUBRIC_ID,
                    REVEAL_RATING,
                    ANYTIME_FEEDBACK_RUBRIC_ID,
                    IS_PEER_FEEDBACK_ATTRIBUTED,
                    SHOW_PEER_FEEDBACK_RATINGS,
                    AUTO_SHARE_PEER_FEEDBACK,
                    HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
                    SHOW_AWARDS_PAGE,
                    SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
                    HIDE_PREVIOUS_QUARTER_REVIEW
                )                               as ROW_HASH
        from source_data as x
)

{% if is_incremental() %}
,organization_history_last_record as(
        select
                x.ID,
                x.NAME,
                x.FEEDBACK_REQUEST_TEMPLATE_ID,
                x.FEEDBACK_REMINDER_TEMPLATE_ID,
                x.PEERFEEDBACK_FORM,
                x.REVIEW_FORM,
                x.SELFEVAL_FORM,
                x.UPWARD_FEEDBACK_RUBRIC,
                x.HIDE_ROLE,
                x.HIDE_OVERALL_RATING,
                x.HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
                x.UPWARD_FEEDBACK_RUBRIC_ID,
                x.REVEAL_RATING,
                x.ANYTIME_FEEDBACK_RUBRIC_ID,
                x.IS_PEER_FEEDBACK_ATTRIBUTED,
                x.SHOW_PEER_FEEDBACK_RATINGS,
                x.AUTO_SHARE_PEER_FEEDBACK,
                x.HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
                x.SHOW_AWARDS_PAGE,
                x.SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
                x.HIDE_PREVIOUS_QUARTER_REVIEW,
                x.ETL_UPDATED_TIMESTAMP,
                x.IS_DELETED_IN_SOURCE,
                x.TIMESTAMP_EFFECTIVE,
                x.ROW_HASH
        from    {{ this }} as x
        qualify row_number() over (partition by x.ID order by x.TIMESTAMP_EFFECTIVE desc) = 1
)
{% endif %}

,final_new_records as (
        select
                ID,
                "NAME",
                FEEDBACK_REQUEST_TEMPLATE_ID,
                FEEDBACK_REMINDER_TEMPLATE_ID,
                PEERFEEDBACK_FORM,
                REVIEW_FORM,
                SELFEVAL_FORM,
                UPWARD_FEEDBACK_RUBRIC,
                HIDE_ROLE,
                HIDE_OVERALL_RATING,
                HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
                UPWARD_FEEDBACK_RUBRIC_ID,
                REVEAL_RATING,
                ANYTIME_FEEDBACK_RUBRIC_ID,
                IS_PEER_FEEDBACK_ATTRIBUTED,
                SHOW_PEER_FEEDBACK_RATINGS,
                AUTO_SHARE_PEER_FEEDBACK,
                HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
                SHOW_AWARDS_PAGE,
                SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
                HIDE_PREVIOUS_QUARTER_REVIEW,
                ETL_UPDATED_TIMESTAMP,
                IS_DELETED_IN_SOURCE,
                TIMESTAMP_EFFECTIVE,
                ROW_HASH
        from    organization_history_hash
{% if is_incremental() %}
        union
        select
                ID,
                "NAME",
                FEEDBACK_REQUEST_TEMPLATE_ID,
                FEEDBACK_REMINDER_TEMPLATE_ID,
                PEERFEEDBACK_FORM,
                REVIEW_FORM,
                SELFEVAL_FORM,
                UPWARD_FEEDBACK_RUBRIC,
                HIDE_ROLE,
                HIDE_OVERALL_RATING,
                HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
                UPWARD_FEEDBACK_RUBRIC_ID,
                REVEAL_RATING,
                ANYTIME_FEEDBACK_RUBRIC_ID,
                IS_PEER_FEEDBACK_ATTRIBUTED,
                SHOW_PEER_FEEDBACK_RATINGS,
                AUTO_SHARE_PEER_FEEDBACK,
                HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
                SHOW_AWARDS_PAGE,
                SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
                HIDE_PREVIOUS_QUARTER_REVIEW,
                ETL_UPDATED_TIMESTAMP,
                IS_DELETED_IN_SOURCE,
                TIMESTAMP_EFFECTIVE,
                ROW_HASH
        from    organization_history_last_record l
        where not exists (
                select 1 from organization_history_hash h
                where l.ID = h.ID
                        and l.TIMESTAMP_EFFECTIVE = h.TIMESTAMP_EFFECTIVE
        )
{% endif %}        
)

,final as (
        select
                ID,
                "NAME",
                FEEDBACK_REQUEST_TEMPLATE_ID,
                FEEDBACK_REMINDER_TEMPLATE_ID,
                PEERFEEDBACK_FORM,
                REVIEW_FORM,
                SELFEVAL_FORM,
                UPWARD_FEEDBACK_RUBRIC,
                HIDE_ROLE,
                HIDE_OVERALL_RATING,
                HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
                UPWARD_FEEDBACK_RUBRIC_ID,
                REVEAL_RATING,
                ANYTIME_FEEDBACK_RUBRIC_ID,
                IS_PEER_FEEDBACK_ATTRIBUTED,
                SHOW_PEER_FEEDBACK_RATINGS,
                AUTO_SHARE_PEER_FEEDBACK,
                HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
                SHOW_AWARDS_PAGE,
                SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
                HIDE_PREVIOUS_QUARTER_REVIEW,
                ETL_UPDATED_TIMESTAMP,
                IS_DELETED_IN_SOURCE,
                TIMESTAMP_EFFECTIVE,
                ROW_HASH,
                case when lag(ROW_HASH) over(partition by ID order by TIMESTAMP_EFFECTIVE)
                        <> ROW_HASH
                        or lag(ROW_HASH) over(partition by ID order by TIMESTAMP_EFFECTIVE) is null
                        then 1
                end as ROW_CHANGE_FILTER
        from final_new_records
        qualify ROW_CHANGE_FILTER = 1
)

select
        {{dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} as organization_key,
        ID,
        "NAME",
        FEEDBACK_REQUEST_TEMPLATE_ID,
        FEEDBACK_REMINDER_TEMPLATE_ID,
        PEERFEEDBACK_FORM,
        REVIEW_FORM,
        SELFEVAL_FORM,
        UPWARD_FEEDBACK_RUBRIC,
        HIDE_ROLE,
        HIDE_OVERALL_RATING,
        HIDE_SELFEVAL_RATING_UNTIL_LOCKED,
        UPWARD_FEEDBACK_RUBRIC_ID,
        REVEAL_RATING,
        ANYTIME_FEEDBACK_RUBRIC_ID,
        IS_PEER_FEEDBACK_ATTRIBUTED,
        SHOW_PEER_FEEDBACK_RATINGS,
        AUTO_SHARE_PEER_FEEDBACK,
        HIDE_SELFEVAL_AND_PEERFEEDBACK_ON_REVIEW,
        SHOW_AWARDS_PAGE,
        SHOW_PRIVATE_FEEDBACK_IN_CALIBRATION,
        HIDE_PREVIOUS_QUARTER_REVIEW,
        ETL_UPDATED_TIMESTAMP,
        IS_DELETED_IN_SOURCE,
        TIMESTAMP_EFFECTIVE,
        ROW_HASH
from final
