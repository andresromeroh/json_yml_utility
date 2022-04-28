{{
    config(
            materialized="incremental",
            unique_key = 'level_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


with source_data as(
        select
                x.ID,
                x.NAME,
                x.DESCRIPTION,
                x.RUBRIC_ID,
                x.SEQ,
                x.IS_SPECIFIED,
                x._H2_SF_INSERTED,
                x._H2_IS_DELETED,
                x._H2_BINLOG_TS_MS
        from {{ idw_package.dynamic_source('feedback_raw', 'ckapp_level_history') }} x
        {% if is_incremental() %}
                where x._H2_SF_INSERTED >= (select max(s.ETL_UPDATED_TIMESTAMP)::date from {{ this }} as s )
        {% endif %}
        qualify row_number() over (partition by x.ID, x._H2_BINLOG_TS_MS order by x._H2_BINLOG_TS_MS asc) = 1
)

,level_history_hash as (
        select
                x.ID::integer                           as ID
                ,x.NAME::string                         as "NAME"
                ,x.DESCRIPTION::string                  as "DESCRIPTION"
                ,x.RUBRIC_ID::integer                   as RUBRIC_ID
                ,x.SEQ::integer                         as SEQ
                ,x.IS_SPECIFIED::boolean                as IS_SPECIFIED
                ,x._H2_SF_INSERTED::timestamp_tz        as ETL_UPDATED_TIMESTAMP
                ,x._H2_IS_DELETED::boolean              as IS_DELETED_IN_SOURCE
                ,x._H2_BINLOG_TS_MS::timestamp_tz       as TIMESTAMP_EFFECTIVE
                ,hash(
                        ID
                        ,"NAME"
                        ,"DESCRIPTION"
                        ,RUBRIC_ID
                        ,SEQ
                        ,IS_SPECIFIED  
                )                               as ROW_HASH
        from source_data as x
)

{% if is_incremental() %}
,level_history_last_record as(
        select
                x.ID
                ,x.NAME
                ,x.DESCRIPTION
                ,x.RUBRIC_ID
                ,x.SEQ
                ,x.IS_SPECIFIED
                ,x.ETL_UPDATED_TIMESTAMP
                ,x.IS_DELETED_IN_SOURCE
                ,x.TIMESTAMP_EFFECTIVE
                ,x.ROW_HASH
        from    {{ this }} as x
        qualify row_number() over (partition by x.ID order by x.TIMESTAMP_EFFECTIVE desc) = 1
)
{% endif %}

,final_new_records as (
        select
                ID
                ,"NAME"
                ,"DESCRIPTION"
                ,RUBRIC_ID
                ,SEQ
                ,IS_SPECIFIED
                ,ETL_UPDATED_TIMESTAMP
                ,IS_DELETED_IN_SOURCE
                ,TIMESTAMP_EFFECTIVE
                ,ROW_HASH
        from    level_history_hash
{% if is_incremental() %}
        union
        select
                ID
                ,"NAME"
                ,"DESCRIPTION"
                ,RUBRIC_ID
                ,SEQ
                ,IS_SPECIFIED
                ,ETL_UPDATED_TIMESTAMP
                ,IS_DELETED_IN_SOURCE
                ,TIMESTAMP_EFFECTIVE
                ,ROW_HASH
        from    level_history_last_record l
        where not exists (
                select 1 from level_history_hash h
                where l.ID = h.ID
                        and l.TIMESTAMP_EFFECTIVE = h.TIMESTAMP_EFFECTIVE
        )
{% endif %}        
)

,final as (
        select
                ID
                ,"NAME"
                ,"DESCRIPTION"
                ,RUBRIC_ID
                ,SEQ
                ,IS_SPECIFIED
                ,ETL_UPDATED_TIMESTAMP
                ,IS_DELETED_IN_SOURCE
                ,TIMESTAMP_EFFECTIVE
                ,ROW_HASH
                ,case when lag(ROW_HASH) over(partition by ID order by TIMESTAMP_EFFECTIVE)
                        <> ROW_HASH
                        or lag(ROW_HASH) over(partition by ID order by TIMESTAMP_EFFECTIVE) is null
                        then 1
                end as ROW_CHANGE_FILTER
        from final_new_records
        qualify ROW_CHANGE_FILTER = 1
)

select
        {{dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} as level_key
        ,ID
        ,"NAME"
        ,"DESCRIPTION"
        ,RUBRIC_ID
        ,SEQ
        ,IS_SPECIFIED
        ,ETL_UPDATED_TIMESTAMP
        ,IS_DELETED_IN_SOURCE
        ,TIMESTAMP_EFFECTIVE
        ,ROW_HASH
from final
