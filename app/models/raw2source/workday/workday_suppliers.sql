{{
config(
    materialized = "incremental",
    unique_key = 'supplier_key',
    tags=['WORKDAY_RAW2SOURCE']
)
}}

with source_data as (
        select
            raw_data as raw_json,
            raw_filename,
            x.raw_inserted_timestamp
        from {{ idw_package.dynamic_source('workday_raw', 'workday_suppliers') }} x
{% if is_incremental() %}
where raw_inserted_timestamp >=
    (select MAX(etl_updated_timestamp)::date from {{ this }} )
{% endif %}
),

raw_parsed as ( 
select
        raw_json:Workday_ID::string as Workday_ID
        , raw_json:Status::string as status
        , raw_json:Last_Updated_Date::date::Timestamp_TZ as last_updated_date
        , raw_json:Created_Moment::date as Created_date
        , raw_json:Report_Effective_Date::date::Timestamp_TZ as timestamp_effective
        , raw_json:Supplier_Category::string as supplier_category
        , raw_json:Supplier_Name::string as supplier_name

from source_data
qualify row_number() over (partition by workday_id, timestamp_effective order by timestamp_effective asc) = 1
),

suppliers_hash as (
select  Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,timestamp_effective
        ,supplier_category
        ,supplier_name,
        hash(Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,supplier_category
        ,supplier_name
        ) as row_hash
from    raw_parsed
)

{% if is_incremental() %}
,suppliers_last_record as
(
select  Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,timestamp_effective
        ,supplier_category
        ,supplier_name
        ,row_hash
        ,row_number() over (partition by  workday_ID order by timestamp_effective desc) rn
from    {{ this }} suppliers
qualify rn = 1
)
{% endif %}

,final_new_records as
(
select  Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,timestamp_effective
        ,supplier_category
        ,supplier_name
        ,row_hash
from    suppliers_hash

{% if is_incremental() %}
union
select Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,timestamp_effective
        ,supplier_category
        ,supplier_name
        ,row_hash
from  suppliers_last_record last_updated_date
where not exists (select 1 from suppliers_hash h
                    where l.workday_ID = h.workday_ID
                    and l.timestamp_effective = h.timestamp_effective)
{% endif %}
)

,final as (
select  Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,timestamp_effective
        ,supplier_category
        ,supplier_name
        ,row_hash
        , case when LAG(row_hash) over(partition by Workday_ID order by timestamp_effective)
            <> row_hash
            or LAG(row_hash) over(partition by Workday_ID order by timestamp_effective) is null
            then 1
        end as row_change_filter
from    final_new_records
qualify row_change_filter = 1
)

select  {{dbt_utils.surrogate_key([ "Workday_ID", "timestamp_effective"]) }} as supplier_key
        ,Workday_ID
        ,status
        ,last_updated_date
        ,created_date
        ,timestamp_effective
        ,supplier_category
        ,supplier_name
        ,current_timestamp() as etl_updated_timestamp
from    final
