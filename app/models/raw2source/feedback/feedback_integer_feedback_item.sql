{{
    config(
            materialized="incremental",
            unique_key = 'integer_feedback_item_key',
            tags=['FEEDBACK','RAW2SOURCE']
    )
}}


WITH source_data AS(
    SELECT
        x.ID,
        x.NAME,
        x.VALUE,
        x.ORDERED_FEEDBACK_ITEM_ID,
        x.DRAFT_VALUE,
        x._H2_SF_INSERTED,
        x._H2_IS_DELETED,
        x._H2_BINLOG_TS_MS
    FROM {{ idw_package.dynamic_source('feedback_raw', 'feedback_integerfeedbackitem_history') }} x
    {% if is_incremental() %}
            WHERE x._H2_SF_INSERTED >= (SELECT MAX(s.ETL_UPDATED_TIMESTAMP)::DATE FROM {{ this }} AS s)
    {% endif %}
    QUALIFY row_number() OVER (PARTITION BY x.ID, x._H2_BINLOG_TS_MS ORDER BY x._H2_BINLOG_TS_MS ASC) = 1
)

,dimension_history_hash AS (
    SELECT
        x.ID::integer                           AS ID
        ,x.NAME::string                         AS "NAME"
        ,x.VALUE::integer                         AS "VALUE"
        ,x.ORDERED_FEEDBACK_ITEM_ID::integer    AS ORDERED_FEEDBACK_ITEM_ID
        ,x.DRAFT_VALUE::integer                   AS DRAFT_VALUE
        ,x._H2_SF_INSERTED::timestamp_tz        AS ETL_UPDATED_TIMESTAMP
        ,x._H2_IS_DELETED::boolean              AS IS_DELETED_IN_SOURCE
        ,x._H2_BINLOG_TS_MS::timestamp_tz       AS TIMESTAMP_EFFECTIVE
        ,HASH(
            ID
            ,"NAME"
            ,"VALUE"
            ,ORDERED_FEEDBACK_ITEM_ID
            ,DRAFT_VALUE
        )                               AS ROW_HASH
    FROM source_data AS x
)

{% if is_incremental() %}
,dimension_history_last_record AS(
    SELECT
        x.ID
        ,x.NAME
        ,x.VALUE
        ,x.ORDERED_FEEDBACK_ITEM_ID
        ,x.DRAFT_VALUE
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
        ,"VALUE"
        ,ORDERED_FEEDBACK_ITEM_ID
        ,DRAFT_VALUE
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
        ,"VALUE"
        ,ORDERED_FEEDBACK_ITEM_ID
        ,DRAFT_VALUE
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
        ,"VALUE"
        ,ORDERED_FEEDBACK_ITEM_ID
        ,DRAFT_VALUE
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
    {{dbt_utils.surrogate_key([ "ID", "TIMESTAMP_EFFECTIVE"]) }} AS integer_feedback_item_key
    ,ID
    ,"NAME"
    ,"VALUE"
    ,ORDERED_FEEDBACK_ITEM_ID
    ,DRAFT_VALUE
    ,ETL_UPDATED_TIMESTAMP
    ,IS_DELETED_IN_SOURCE
    ,TIMESTAMP_EFFECTIVE
    ,ROW_HASH
FROM final