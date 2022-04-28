{{
    config(
            materialized="incremental",
            unique_key = 'transaction_key',
            tags=['WORKDAY_RAW2SOURCE']
    )
}}

WITH source_data AS (
    SELECT
        x.raw_data AS raw_json
        , x.raw_filename
        , x.raw_inserted_timestamp
    FROM {{ idw_package.dynamic_source('workday_raw', 'workday_transactions') }} AS x

    {% if is_incremental() %}
WHERE raw_inserted_timestamp >=
    (SELECT MAX(etl_updated_timestamp)::DATE FROM {{ this }} )
    {% endif %}
)

raw_parsed as (
select   raw_json:Business_Process_ID::string                   as Business_Process_ID
         ,raw_json:is_Worker_BP::boolean                        as is_Worker_BP
         ,raw_json:is_Staffing_BP::boolean                      as is_Staffing_BP
         ,raw_json:is_Job_Requisition_BP::boolean               as is_Job_Requisition_BP
         ,raw_json:is_Personal_Information_BP::boolean          as is_Personal_Information_BP
         ,raw_json:is_Standalone_BP::boolean                    as is_Standalone_BP
         ,raw_json:Business_Process_Name::string                as Business_Process_Name
         ,raw_json:Business_Process_Reason_Category::string     as Business_Process_Reason_Category
         ,raw_json:Business_Process_Reason::string              as Business_Process_Reason
         ,raw_json:Business_Process_Type::string                as Business_Process_Type
         ,raw_json:Part_of_Business_Process::string             as Part_of_Business_Process
         ,raw_json:Date_Time_Completed::date::Timestamp_TZ      as Date_Time_Completed
         ,raw_json:Date_Rescinded::date                         as Date_Rescinded
         ,raw_json:Corrected::string                            as Corrected
         ,raw_json:Last_Correction_Event_Date::date             as Last_Correction_Event_Date
         ,raw_json:Secondary_Termination_Reasons::string        as Secondary_Termination_Reasons
         ,raw_json:Date_of_Birth::date                          as Date_of_Birth
         ,raw_json:Gender::string                               as Gender
         ,raw_json:Ethnicity::string                            as Ethnicity
         ,raw_json:Pronouns::string                             as Pronouns
         ,raw_json:Gender_Identity::string                      as Gender_Identity
         ,raw_json:Sexual_Orientation::string                   as Sexual_Orientation
         ,raw_json:Disability::string                           as Disability
         ,raw_json:Military_Status::string                      as Military_Status
         ,raw_json:Employee_ID::string                          as Employee_ID
         ,raw_json:Position_ID::string                          as Position_ID
         ,raw_json:Business_Title::string                       as Business_Title
         ,raw_json:Work_Space_Name::string                      as Work_Space_Name
         ,raw_json:Work_Space_ID::string                        as Work_Space_ID
         ,raw_json:Pay_Rate_Type::string                        as Pay_Rate_Type
         ,raw_json:Time_Type::string                            as Time_Type
         ,raw_json:Requisition_ID::string                       as Requisition_ID
         ,raw_json:Location::string                             as Location
         ,raw_json:Job_Profile::string                          as Job_Profile
         ,raw_json:Work_Email_Address::string                   as Work_Email_Address
         ,raw_json:Budgeted_Start_Date::date                    as Budgeted_Start_Date
         ,raw_json:Finance_Only_Start_Date::date                as Finance_Only_Start_Date
         ,raw_json:Position_Job_Profile::string                 as Position_Job_Profile
         ,raw_json:Position_Location::string                    as Position_Location
         ,raw_json:Position_Time_Type::string                   as Position_Time_Type
         ,raw_json:Position_Title::string                       as Position_Title
         ,raw_json:Position_Worker_Type::string                 as Position_Worker_Type
         ,raw_json:TA_Forecasted_Start_Date::date               as TA_Forecasted_Start_Date
         ,raw_json:Employee_Contingent_Worker_Type::string      as Employee_Contingent_Worker_Type
         ,raw_json:LDAP::string                                 as LDAP
         ,raw_json:Public_Pronoun::string                       as Public_Pronoun
         ,raw_json:Job_Requisition_Organizations_group::variant as Job_Requisition_Organizations_group
         ,raw_json:Worker_Position_Organizations_group::variant as Worker_Position_Organizations_group
         ,raw_json:Effective_Date::date                         as timestamp_effective

FROM    source_data

QUALIFY row_number() OVER (PARTITION BY Business_Process_ID, timestamp_effective ORDER BY timestamp_effective ASC) = 1
),

Worker_Position_Organizations_Cte as (
  select    Business_Process_ID,
            parse_json(Worker_Position_Organizations_group) as o
  from      raw_parsed
),

Worker_Position_Organizations_flattened as (
select  Worker_Position_Organizations_Cte.Business_Process_ID,
        flattened.value:Workday_ID::string as Workday_ID,
        flattened.value:Type::string as Type
from    Worker_Position_Organizations_Cte,
        lateral flatten( input => o ) as flattened
) ,

source_data_flattened as (
select    r.Business_Process_ID
         ,r.is_Worker_BP
         ,r.is_Staffing_BP
         ,r.is_Job_Requisition_BP
         ,r.is_Personal_Information_BP
         ,r.is_Standalone_BP
         ,r.Business_Process_Name
         ,r.Business_Process_Reason_Category
         ,r.Business_Process_Reason
         ,r.Business_Process_Type
         ,r.Part_of_Business_Process
         ,r.Date_Time_Completed
         ,r.Date_Rescinded
         ,r.Corrected
         ,r.Last_Correction_Event_Date
         ,r.Secondary_Termination_Reasons
         ,r.Date_of_Birth
         ,r.Gender
         ,r.Ethnicity
         ,r.Pronouns
         ,r.Gender_Identity
         ,r.Sexual_Orientation
         ,r.Disability
         ,r.Military_Status
         ,r.Employee_ID
         ,r.Position_ID
         ,r.Business_Title
         ,r.Work_Space_Name
         ,r.Work_Space_ID
         ,r.Pay_Rate_Type
         ,r.Time_Type
         ,r.Requisition_ID
         ,r.Location
         ,r.Job_Profile
         ,r.Work_Email_Address
         ,r.Budgeted_Start_Date
         ,r.Finance_Only_Start_Date
         ,r.Position_Job_Profile
         ,r.Position_Location
         ,r.Position_Time_Type
         ,r.Position_Title
         ,r.Position_Worker_Type
         ,r.TA_Forecasted_Start_Date
         ,r.Employee_Contingent_Worker_Type
         ,r.LDAP
         ,r.Public_Pronoun
         ,r.Job_Requisition_Organizations_group
         ,r.Worker_Position_Organizations_group
         ,r.timestamp_effective
         ,position_company.workday_id as Worker_Position_Company
         ,position_cost_center.workday_id as Worker_Position_Cost_Center
         ,position_functional_tier.workday_id as Worker_Position_Functional_Tier
         ,position_market.workday_id as Worker_Position_Market
         ,position_pay_group.workday_id as Worker_Position_Pay_Group
         ,position_segment.workday_id as Worker_Position_Segment
         ,position_supervisory.workday_id as Worker_Position_Supervisory
         ,position_vertical.workday_id as Worker_Position_Vertical
FROM     raw_parsed r
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Company'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_company
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Cost Center'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_cost_center
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Functional Tier'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_functional_tier
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Market'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_market
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Pay Group'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_pay_group
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Segment'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_segment
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Supervisory'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_supervisory
LEFT JOIN  LATERAL (SELECT  workday_id
                    FROM    Worker_Position_Organizations_flattened f
                    WHERE   type = 'Vertical'
                    AND     f.Business_Process_ID = r.Business_Process_ID
                    ) position_vertical
) ,

transaction_hash AS (
select  Business_Process_ID
        ,is_Worker_BP
        ,is_Staffing_BP
        ,is_Job_Requisition_BP
        ,is_Personal_Information_BP
        ,is_Standalone_BP
        ,Business_Process_Name
        ,Business_Process_Reason_Category
        ,Business_Process_Reason
        ,Business_Process_Type
        ,Part_of_Business_Process
        ,Date_Time_Completed
        ,Date_Rescinded
        ,Corrected
        ,Last_Correction_Event_Date
        ,Secondary_Termination_Reasons
        ,Date_of_Birth
        ,Gender
        ,Ethnicity
        ,Pronouns
        ,Gender_Identity
        ,Sexual_Orientation
        ,Disability
        ,Military_Status
        ,Employee_ID
        ,Position_ID
        ,Business_Title
        ,Work_Space_Name
        ,Work_Space_ID
        ,Pay_Rate_Type
        ,Time_Type
        ,Requisition_ID
        ,Location
        ,Job_Profile
        ,Work_Email_Address
        ,Job_Requisition_Organizations_group
        ,Worker_Position_Organizations_group
        ,Worker_Position_Company
        ,Worker_Position_Cost_Center
        ,Worker_Position_Functional_Tier
        ,Worker_Position_Market
        ,Worker_Position_Pay_Group
        ,Worker_Position_Segment
        ,Worker_Position_Supervisory
        ,Worker_Position_Vertical
        ,Budgeted_Start_Date
        ,Finance_Only_Start_Date
        ,Position_Job_Profile
        ,Position_Location
        ,Position_Time_Type
        ,Position_Title
        ,Position_Worker_Type
        ,TA_Forecasted_Start_Date
        ,Employee_Contingent_Worker_Type
        ,LDAP
        ,Public_Pronoun
        ,timestamp_effective
        ,HASH(Business_Process_ID
        ,is_Worker_BP
        ,is_Staffing_BP
        ,is_Job_Requisition_BP
        ,is_Personal_Information_BP
        ,is_Standalone_BP
        ,Business_Process_Name
        ,Business_Process_Reason_Category
        ,Business_Process_Reason
        ,Business_Process_Type
        ,Part_of_Business_Process
        ,Date_Time_Completed
        ,Date_Rescinded
        ,Corrected
        ,Last_Correction_Event_Date
        ,Secondary_Termination_Reasons
        ,Date_of_Birth
        ,Gender
        ,Ethnicity
        ,Pronouns
        ,Gender_Identity
        ,Sexual_Orientation
        ,Disability
        ,Military_Status
        ,Employee_ID
        ,Position_ID
        ,Business_Title
        ,Work_Space_Name
        ,Work_Space_ID
        ,Pay_Rate_Type
        ,Time_Type
        ,Requisition_ID
        ,Location
        ,Job_Profile
        ,Work_Email_Address
        ,Job_Requisition_Organizations_group
        ,Worker_Position_Organizations_group
        ,Worker_Position_Company
        ,Worker_Position_Cost_Center
        ,Worker_Position_Functional_Tier
        ,Worker_Position_Market
        ,Worker_Position_Pay_Group
        ,Worker_Position_Segment
        ,Worker_Position_Supervisory
        ,Worker_Position_Vertical
        ,Budgeted_Start_Date
        ,Finance_Only_Start_Date
        ,Position_Job_Profile
        ,Position_Location
        ,Position_Time_Type
        ,Position_Title
        ,Position_Worker_Type
        ,TA_Forecasted_Start_Date
        ,Employee_Contingent_Worker_Type
        ,LDAP
        ,Public_Pronoun
        ) as row_hash

from  source_data_flattened
)

{% if is_incremental() %}
,last_record as
(
select  Business_Process_ID
        ,is_Worker_BP
        ,is_Staffing_BP
        ,is_Job_Requisition_BP
        ,is_Personal_Information_BP
        ,is_Standalone_BP
        ,Business_Process_Name
        ,Business_Process_Reason_Category
        ,Business_Process_Reason
        ,Business_Process_Type
        ,Part_of_Business_Process
        ,Date_Time_Completed
        ,Date_Rescinded
        ,Corrected
        ,Last_Correction_Event_Date
        ,Secondary_Termination_Reasons
        ,Date_of_Birth
        ,Gender
        ,Ethnicity
        ,Pronouns
        ,Gender_Identity
        ,Sexual_Orientation
        ,Disability
        ,Military_Status
        ,Employee_ID
        ,Position_ID
        ,Business_Title
        ,Work_Space_Name
        ,Work_Space_ID
        ,Pay_Rate_Type
        ,Time_Type
        ,Requisition_ID
        ,Location
        ,Job_Profile
        ,Work_Email_Address
        ,Job_Requisition_Organizations_group
        ,Worker_Position_Organizations_group
        ,Worker_Position_Company
        ,Worker_Position_Cost_Center
        ,Worker_Position_Functional_Tier
        ,Worker_Position_Market
        ,Worker_Position_Pay_Group
        ,Worker_Position_Segment
        ,Worker_Position_Supervisory
        ,Worker_Position_Vertical
        ,Budgeted_Start_Date
        ,Finance_Only_Start_Date
        ,Position_Job_Profile
        ,Position_Location
        ,Position_Time_Type
        ,Position_Title
        ,Position_Worker_Type
        ,TA_Forecasted_Start_Date
        ,Employee_Contingent_Worker_Type
        ,LDAP
        ,Public_Pronoun
        ,timestamp_effective
        ,row_hash
from    {{ this }} transactions
qualify row_number() over (partition by Business_Process_ID order by timestamp_effective desc) = 1
)
{% endif %}


,final_new_records as
(
select  Business_Process_ID
        ,is_Worker_BP
        ,is_Staffing_BP
        ,is_Job_Requisition_BP
        ,is_Personal_Information_BP
        ,is_Standalone_BP
        ,Business_Process_Name
        ,Business_Process_Reason_Category
        ,Business_Process_Reason
        ,Business_Process_Type
        ,Part_of_Business_Process
        ,Date_Time_Completed
        ,Date_Rescinded
        ,Corrected
        ,Last_Correction_Event_Date
        ,Secondary_Termination_Reasons
        ,Date_of_Birth
        ,Gender
        ,Ethnicity
        ,Pronouns
        ,Gender_Identity
        ,Sexual_Orientation
        ,Disability
        ,Military_Status
        ,Employee_ID
        ,Position_ID
        ,Business_Title
        ,Work_Space_Name
        ,Work_Space_ID
        ,Pay_Rate_Type
        ,Time_Type
        ,Requisition_ID
        ,Location
        ,Job_Profile
        ,Work_Email_Address
        ,Job_Requisition_Organizations_group
        ,Worker_Position_Organizations_group
        ,Worker_Position_Company
        ,Worker_Position_Cost_Center
        ,Worker_Position_Functional_Tier
        ,Worker_Position_Market
        ,Worker_Position_Pay_Group
        ,Worker_Position_Segment
        ,Worker_Position_Supervisory
        ,Worker_Position_Vertical
        ,Budgeted_Start_Date
        ,Finance_Only_Start_Date
        ,Position_Job_Profile
        ,Position_Location
        ,Position_Time_Type
        ,Position_Title
        ,Position_Worker_Type
        ,TA_Forecasted_Start_Date
        ,Employee_Contingent_Worker_Type
        ,LDAP
        ,Public_Pronoun
        ,timestamp_effective
        ,row_hash
FROM    transaction_hash

    {% if is_incremental() %}
  UNION
  select Business_Process_ID
         ,is_Worker_BP
         ,is_Staffing_BP
         ,is_Job_Requisition_BP
         ,is_Personal_Information_BP
         ,is_Standalone_BP
         ,Business_Process_Name
         ,Business_Process_Reason_Category
         ,Business_Process_Reason
         ,Business_Process_Type
         ,Part_of_Business_Process
         ,Date_Time_Completed
         ,Date_Rescinded
         ,Corrected
         ,Last_Correction_Event_Date
         ,Secondary_Termination_Reasons
         ,Date_of_Birth
         ,Gender
         ,Ethnicity
         ,Pronouns
         ,Gender_Identity
         ,Sexual_Orientation
         ,Disability
         ,Military_Status
         ,Employee_ID
         ,Position_ID
         ,Business_Title
         ,Work_Space_Name
         ,Work_Space_ID
         ,Pay_Rate_Type
         ,Time_Type
         ,Requisition_ID
         ,Location
         ,Job_Profile
         ,Work_Email_Address
         ,Job_Requisition_Organizations_group
         ,Worker_Position_Organizations_group
         ,Worker_Position_Company
         ,Worker_Position_Cost_Center
         ,Worker_Position_Functional_Tier
         ,Worker_Position_Market
         ,Worker_Position_Pay_Group
         ,Worker_Position_Segment
         ,Worker_Position_Supervisory
         ,Worker_Position_Vertical
         ,Budgeted_Start_Date
         ,Finance_Only_Start_Date
         ,Position_Job_Profile
         ,Position_Location
         ,Position_Time_Type
         ,Position_Title
         ,Position_Worker_Type
         ,TA_Forecasted_Start_Date
         ,Employee_Contingent_Worker_Type
         ,LDAP
         ,Public_Pronoun
         ,timestamp_effective
         ,row_hash
  from  last_record l
  WHERE NOT EXISTS (SELECT 1 FROM transaction_hash h
                    WHERE l.Business_Process_ID = h.Business_Process_ID
                    AND l.timestamp_effective = h.timestamp_effective)
{% endif %}
)

,final as (
select  Business_Process_ID
        ,is_Worker_BP
        ,is_Staffing_BP
        ,is_Job_Requisition_BP
        ,is_Personal_Information_BP
        ,is_Standalone_BP
        ,Business_Process_Name
        ,Business_Process_Reason_Category
        ,Business_Process_Reason
        ,Business_Process_Type
        ,Part_of_Business_Process
        ,Date_Time_Completed
        ,Date_Rescinded
        ,Corrected
        ,Last_Correction_Event_Date
        ,Secondary_Termination_Reasons
        ,Date_of_Birth
        ,Gender
        ,Ethnicity
        ,Pronouns
        ,Gender_Identity
        ,Sexual_Orientation
        ,Disability
        ,Military_Status
        ,Employee_ID
        ,Position_ID
        ,Business_Title
        ,Work_Space_Name
        ,Work_Space_ID
        ,Pay_Rate_Type
        ,Time_Type
        ,Requisition_ID
        ,Location
        ,Job_Profile
        ,Work_Email_Address
        ,Job_Requisition_Organizations_group
        ,Worker_Position_Organizations_group
        ,Worker_Position_Company
        ,Worker_Position_Cost_Center
        ,Worker_Position_Functional_Tier
        ,Worker_Position_Market
        ,Worker_Position_Pay_Group
        ,Worker_Position_Segment
        ,Worker_Position_Supervisory
        ,Worker_Position_Vertical
        ,Budgeted_Start_Date
        ,Finance_Only_Start_Date
        ,Position_Job_Profile
        ,Position_Location
        ,Position_Time_Type
        ,Position_Title
        ,Position_Worker_Type
        ,TA_Forecasted_Start_Date
        ,Employee_Contingent_Worker_Type
        ,LDAP
        ,Public_Pronoun
        ,timestamp_effective
        ,row_hash
        , CASE WHEN LAG(row_hash) OVER(PARTITION BY Business_Process_ID ORDER BY timestamp_effective)
            <> row_hash
            OR LAG(row_hash) OVER(PARTITION BY Business_Process_ID ORDER BY timestamp_effective) IS NULL
            THEN 1
        END AS row_change_filter
    FROM final_new_records
    QUALIFY row_change_filter = 1
)

select   {{dbt_utils.surrogate_key([ "Business_Process_ID", "timestamp_effective"]) }} AS transaction_key
        ,Business_Process_ID
        ,is_Worker_BP
        ,is_Staffing_BP
        ,is_Job_Requisition_BP
        ,is_Personal_Information_BP
        ,is_Standalone_BP
        ,Business_Process_Name
        ,Business_Process_Reason_Category
        ,Business_Process_Reason
        ,Business_Process_Type
        ,Part_of_Business_Process
        ,Date_Time_Completed
        ,Date_Rescinded
        ,Corrected
        ,Last_Correction_Event_Date
        ,Secondary_Termination_Reasons
        ,Date_of_Birth
        ,Gender
        ,Ethnicity
        ,Pronouns
        ,Gender_Identity
        ,Sexual_Orientation
        ,Disability
        ,Military_Status
        ,Employee_ID
        ,Position_ID
        ,Business_Title
        ,Work_Space_Name
        ,Work_Space_ID
        ,Pay_Rate_Type
        ,Time_Type
        ,Requisition_ID
        ,Location
        ,Job_Profile
        ,Work_Email_Address
        ,Job_Requisition_Organizations_group
        ,Worker_Position_Organizations_group
        ,Worker_Position_Company
        ,Worker_Position_Cost_Center
        ,Worker_Position_Functional_Tier
        ,Worker_Position_Market
        ,Worker_Position_Pay_Group
        ,Worker_Position_Segment
        ,Worker_Position_Supervisory
        ,Worker_Position_Vertical
        ,Budgeted_Start_Date
        ,Finance_Only_Start_Date
        ,Position_Job_Profile
        ,Position_Location
        ,Position_Time_Type
        ,Position_Title
        ,Position_Worker_Type
        ,TA_Forecasted_Start_Date
        ,Employee_Contingent_Worker_Type
        ,LDAP
        ,Public_Pronoun
        ,timestamp_effective
        ,row_hash
        ,current_timestamp() as etl_updated_timestamp
from    final
