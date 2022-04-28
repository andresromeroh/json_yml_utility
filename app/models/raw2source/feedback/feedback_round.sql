{{
    config(
            materialized="incremental",
            unique_key = 'round_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


with source_data as(
        select
                x.ID,
                x.NAME,
                x.SLUG,
                x.CREATED,
                x.LAST_UPDATED,
                x.PEER_FEEDBACK_DUE,
                x.REVIEWS_DUE,
                x.SELF_EVALS_DUE,
                x.LOCKED,
                x._H2_SF_INSERTED,
                x._H2_IS_DELETED,
                x._H2_BINLOG_TS_MS
        from {{ idw_package.dynamic_source('feedback_raw', 'feedback_round_history') }} x
        {% if is_incremental() %}
                where x._H2_SF_INSERTED >= (select max(s.ETL_UPDATED_TIMESTAMP)::date from {{ this }} as s )
        {% endif %}
        qualify row_number() over (partition by x.ID, x._H2_BINLOG_TS_MS order by x._H2_BINLOG_TS_MS asc) = 1
)

,round_history_hash as (
        select
                x.ID::integer                               as ID
                ,x.NAME::string                             as "NAME"
                ,x.SLUG::string                             as SLUG
                ,x.CREATED::timestamp_tz                    as CREATED_DATE
                ,x.LAST_UPDATED::timestamp_tz               as LAST_UPDATED_DATE
                ,x.PEER_FEEDBACK_DUE::timestamp_tz          as PEER_FEEDBACK_DUE_DATE
                ,x.REVIEWS_DUE::timestamp_tz                as REVIEWS_DUE_DATE
                ,x.SELF_EVALS_DUE::timestamp_tz             as SELF_EVALS_DUE_DATE
                ,x.LOCKED::integer                          as IS_LOCKED
                ,x._H2_SF_INSERTED::timestamp_tz            as ETL_UPDATED_TIMESTAMP
                ,x._H2_IS_DELETED::boolean                  as IS_DELETED_IN_SOURCE
                ,x._H2_BINLOG_TS_MS::timestamp_tz           as TIMESTAMP_EFFECTIVE
                ,hash(
                    ID,
                    "NAME",
                    SLUG,
                    CREATED_DATE,
                    LAST_UPDATED_DATE,
                    PEER_FEEDBACK_DUE_DATE,
                    REVIEWS_DUE_DATE,
                    SELF_EVALS_DUE_DATE,
                    IS_LOCKED
                )                               as ROW_HASH
        from source_data as x
)

{% if is_incremental() %}
,round_history_last_record as(
        select
                x.ID,
                x.NAME,
                x.SLUG,
                x.CREATED_DATE,
                x.LAST_UPDATED_DATE,
                x.PEER_FEEDBACK_DUE_DATE,
                x.REVIEWS_DUE_DATE,
                x.SELF_EVALS_DUE_DATE,
                x.IS_LOCKED,
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
                SLUG,
                CREATED_DATE,
                LAST_UPDATED_DATE,
                PEER_FEEDBACK_DUE_DATE,
                REVIEWS_DUE_DATE,
                SELF_EVALS_DUE_DATE,
                IS_LOCKED,
                ETL_UPDATED_TIMESTAMP,
                IS_DELETED_IN_SOURCE,
                TIMESTAMP_EFFECTIVE,
                ROW_HASH
        from    round_history_hash
{% if is_incremental() %}
        union
        select
                ID,
                "NAME",
                SLUG,
                CREATED_DATE,
                LAST_UPDATED_DATE,
                PEER_FEEDBACK_DUE_DATE,
                REVIEWS_DUE_DATE,
                SELF_EVALS_DUE_DATE,
                IS_LOCKED,
                ETL_UPDATED_TIMESTAMP,
                IS_DELETED_IN_SOURCE,
                TIMESTAMP_EFFECTIVE,
                ROW_HASH
        from    round_history_last_record l
        where not exists (
                select 1 from round_history_hash h
                where l.ID = h.ID
                        and l.TIMESTAMP_EFFECTIVE = h.TIMESTAMP_EFFECTIVE
        )
{% endif %}        
)

,final as (
        select
                ID,
                "NAME",
                SLUG,
                CREATED_DATE,
                LAST_UPDATED_DATE,
                PEER_FEEDBACK_DUE_DATE,
                REVIEWS_DUE_DATE,
                SELF_EVALS_DUE_DATE,
                IS_LOCKED,
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
        {{dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} as round_key,
        ID,
        "NAME",
        SLUG,
        CREATED_DATE,
        LAST_UPDATED_DATE,
        PEER_FEEDBACK_DUE_DATE,
        REVIEWS_DUE_DATE,
        SELF_EVALS_DUE_DATE,
        IS_LOCKED,
        ETL_UPDATED_TIMESTAMP,
        IS_DELETED_IN_SOURCE,
        TIMESTAMP_EFFECTIVE,
        ROW_HASH
from final
