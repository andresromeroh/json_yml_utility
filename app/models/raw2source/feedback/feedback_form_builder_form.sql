{{
    config(
            materialized="incremental",
            unique_key = 'form_builder_form_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


WITH source_data AS(
        SELECT
                x.ID,
                x.NAME,
                x.TYPE,
                x.STATUS,
                x.CREATED_ON,
                x.IS_DELETED,
                PARSE_JSON(x.FORM_STRUCTURE) AS FORM_STRUCTURE,
                PARSE_JSON(x.META)           AS META,
                PARSE_JSON(x.META):lastModifiedBy::string as LAST_MODIFIED_BY,
                x.CREATED_BY_ID,
                x.IS_ARCHIVED,
                x.MODIFIED_BY_ID,
                x.MODIFIED_ON,
                x._H2_SF_INSERTED,
                x._H2_IS_DELETED,
                x._H2_BINLOG_TS_MS
        FROM {{ idw_package.dynamic_source('feedback_raw', 'form_builder_form_history') }} x
        {% if is_incremental() %}
                WHERE x._H2_SF_INSERTED >= (SELECT MAX(s.ETL_UPDATED_TIMESTAMP)::DATE FROM {{ this }} AS s)
        {% endif %}
        QUALIFY row_number() OVER (PARTITION BY x.ID, x._H2_BINLOG_TS_MS ORDER BY x._H2_BINLOG_TS_MS ASC) = 1
)

,dimension_history_hash AS (
        SELECT
                x.ID::integer                           AS ID
                ,x.NAME::string                         AS "NAME"
                ,x.TYPE::integer                        AS "TYPE"
                ,x.STATUS::integer                      AS "STATUS"
                ,x.CREATED_ON::timestamp_tz             AS CREATED_ON
                ,x.IS_DELETED::boolean                  AS IS_DELETED
                ,x.FORM_STRUCTURE::variant              AS FORM_STRUCTURE
                ,x.META::variant                        AS META
                ,x.LAST_MODIFIED_BY::string             AS LAST_MODIFIED_BY
                ,x.CREATED_BY_ID::integer               AS CREATED_BY_ID
                ,x.IS_ARCHIVED::boolean                 AS IS_ARCHIVED
                ,x.MODIFIED_BY_ID::integer              AS MODIFIED_BY_ID
                ,x.MODIFIED_ON::timestamp_tz            AS MODIFIED_ON
                ,x._H2_SF_INSERTED::timestamp_tz        AS ETL_UPDATED_TIMESTAMP
                ,x._H2_IS_DELETED::boolean              AS IS_DELETED_IN_SOURCE
                ,x._H2_BINLOG_TS_MS::timestamp_tz       AS TIMESTAMP_EFFECTIVE
                ,HASH(
                        ID
                        ,"NAME"
                        ,"TYPE"
                        ,"STATUS"
                        ,CREATED_ON
                        ,IS_DELETED
                        ,FORM_STRUCTURE
                        ,META
                        ,LAST_MODIFIED_BY
                        ,CREATED_BY_ID
                        ,IS_ARCHIVED
                        ,MODIFIED_BY_ID
                        ,MODIFIED_ON 
                )                               AS ROW_HASH
        FROM source_data AS x
)

{% if is_incremental() %}
,dimension_history_last_record AS(
        SELECT
                x.ID
                ,x.NAME
                ,x.TYPE
                ,x.STATUS
                ,x.CREATED_ON
                ,x.IS_DELETED
                ,x.FORM_STRUCTURE
                ,x.META
                ,x.LAST_MODIFIED_BY
                ,x.CREATED_BY_ID
                ,x.IS_ARCHIVED
                ,x.MODIFIED_BY_ID
                ,x.MODIFIED_ON
                ,x.ETL_UPDATED_TIMESTAMP
                ,x.IS_DELETED_IN_SOURCE
                ,x.TIMESTAMP_EFFECTIVE
                ,x.ROW_HASH
        FROM    {{ this }} AS x
        QUALIFY row_number() OVER (PARTITION BY x.ID ORDER BY x.TIMESTAMP_EFFECTIVE DESC) = 1
)
{% endif %}

,final_new_records AS (
        SELECT
                ID
                ,"NAME"
                ,"TYPE"
                ,"STATUS"
                ,CREATED_ON
                ,IS_DELETED
                ,FORM_STRUCTURE
                ,META
                ,LAST_MODIFIED_BY
                ,CREATED_BY_ID
                ,IS_ARCHIVED
                ,MODIFIED_BY_ID
                ,MODIFIED_ON 
                ,ETL_UPDATED_TIMESTAMP
                ,IS_DELETED_IN_SOURCE
                ,TIMESTAMP_EFFECTIVE
                ,ROW_HASH
        FROM    dimension_history_hash
{% if is_incremental() %}
        UNION
        SELECT
                ID
                ,"NAME"
                ,"TYPE"
                ,"STATUS"
                ,CREATED_ON
                ,IS_DELETED
                ,FORM_STRUCTURE
                ,META
                ,LAST_MODIFIED_BY
                ,CREATED_BY_ID
                ,IS_ARCHIVED
                ,MODIFIED_BY_ID
                ,MODIFIED_ON 
                ,ETL_UPDATED_TIMESTAMP
                ,IS_DELETED_IN_SOURCE
                ,TIMESTAMP_EFFECTIVE
                ,ROW_HASH
        FROM    dimension_history_last_record l
        WHERE NOT EXISTS (
                SELECT 1 FROM dimension_history_hash h
                WHERE l.ID = h.ID
                        AND l.TIMESTAMP_EFFECTIVE = h.TIMESTAMP_EFFECTIVE
        )
{% endif %}        
)

,final AS (
        SELECT
                ID
                ,"NAME"
                ,"TYPE"
                ,"STATUS"
                ,CREATED_ON
                ,IS_DELETED
                ,FORM_STRUCTURE
                ,META
                ,LAST_MODIFIED_BY
                ,CREATED_BY_ID
                ,IS_ARCHIVED
                ,MODIFIED_BY_ID
                ,MODIFIED_ON
                ,ETL_UPDATED_TIMESTAMP
                ,IS_DELETED_IN_SOURCE
                ,TIMESTAMP_EFFECTIVE
                ,ROW_HASH
                ,CASE WHEN LAG(ROW_HASH) OVER(PARTITION BY ID ORDER BY TIMESTAMP_EFFECTIVE)
                        <> ROW_HASH
                        OR LAG(ROW_HASH) OVER(PARTITION BY ID ORDER BY TIMESTAMP_EFFECTIVE) IS NULL
                        THEN 1
                END AS ROW_CHANGE_FILTER
        FROM final_new_records
        QUALIFY ROW_CHANGE_FILTER = 1
)

SELECT
        {{dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} AS form_builder_form_key
        ,ID
        ,"NAME"
        ,"TYPE"
        ,"STATUS"
        ,CREATED_ON
        ,IS_DELETED
        ,FORM_STRUCTURE
        ,META
        ,LAST_MODIFIED_BY
        ,CREATED_BY_ID
        ,IS_ARCHIVED
        ,MODIFIED_BY_ID
        ,MODIFIED_ON 
        ,ETL_UPDATED_TIMESTAMP
        ,IS_DELETED_IN_SOURCE
        ,TIMESTAMP_EFFECTIVE
        ,ROW_HASH
FROM final