{{
  config(
    materialized = "table",
    tags=['icims_raw2source']
  )
}}
with source_flatten_data as
(SELECT DISTINCT
      d.raw_data:deleted::VARCHAR AS deleted
    , d.raw_data:id::INTEGER AS id
    , d.raw_data:lastUpdated::TIMESTAMP AS lastUpdated
    , flatten_fields.value:name flatten_name
    , flatten_fields.value:type flatten_type
    , flatten_fields.value:value flatten_value
FROM {{ idw_package.dynamic_source('icims_raw', 'submittal') }} d
, LATERAL FLATTEN (input => d.raw_data:fields::VARIANT) as flatten_fields

{% if is_incremental() %}
WHERE raw_inserted_timestamp >= (SELECT max(etl_updated_timestamp) FROM {{ this }})
{% endif %}

)
, source_data as (
    SELECT
      deleted
    , id
    , lastUpdated, max(case when flatten_name = 'sourceperson' then flatten_value:id::INTEGER end) AS sourceperson
    , max(case when flatten_name = 'baseprofile' then flatten_value:id::INTEGER end) AS baseprofile
    , max(case when flatten_name = 'field57146' then flatten_value::STRING end) AS field57146
    , max(case when flatten_name = 'field31565' then flatten_value:defaultAttributes:Label::STRING end) AS field31565
    , max(case when flatten_name = 'field31541' then flatten_value::STRING end) AS field31541
    , max(case when flatten_name = 'field28057' then flatten_value::VARIANT end) AS field28057
    , max(case when flatten_name = 'field31605' then flatten_value::VARIANT end) AS field31605
    , max(case when flatten_name = 'field31536' then flatten_value::VARIANT end) AS field31536
    , max(case when flatten_name = 'field29359' then flatten_value::VARIANT end) AS field29359
    , max(case when flatten_name = 'field29324' then flatten_value:defaultAttributes:Label::STRING end) AS field29324
    , max(case when flatten_name = 'field31532' then flatten_value:id::INTEGER end) AS field31532
    , max(case when flatten_name = 'field28014' then flatten_value:defaultAttributes:Label::STRING end) AS field28014
    , max(case when flatten_name = 'field31538' then flatten_value::STRING end) AS field31538
    , max(case when flatten_name = 'field72067' then flatten_value:defaultAttributes:Label::STRING end) AS field72067
    , max(case when flatten_name = 'sourceexternalid' then flatten_value::STRING end) AS sourceexternalid
    , max(case when flatten_name = 'field59292' then flatten_value:defaultAttributes:Label::STRING end) AS field59292
    , max(case when flatten_name = 'sourcename' then flatten_value::STRING end) AS sourcename
    , max(case when flatten_name = 'createdby' then flatten_value:id::INTEGER end) AS createdby
    , max(case when flatten_name = 'quicksearch' then flatten_value::STRING end) AS quicksearch
    , max(case when flatten_name = 'field57213' then flatten_value:defaultAttributes:Label::STRING end) AS field57213
    , max(case when flatten_name = 'field57201' then flatten_value::STRING end) AS field57201
    , max(case when flatten_name = 'assessmentresults' then flatten_value::VARIANT end) AS assessmentresults
    , max(case when flatten_name = 'field31529' then flatten_value:defaultAttributes:Label::STRING end) AS field31529
    , max(case when flatten_name = 'field31141' then flatten_value:defaultAttributes:Label::STRING end) AS field31141
    , max(case when flatten_name = 'field29330' then flatten_value::STRING end) AS field29330
    , max(case when flatten_name = 'field58091' then flatten_value::VARIANT end) AS field58091
    , max(case when flatten_name = 'field31057' then flatten_value::VARIANT end) AS field31057
    , max(case when flatten_name = 'field71367' then flatten_value::NUMBER end) AS field71367
    , max(case when flatten_name = 'field56940' then flatten_value:defaultAttributes:Label::STRING end) AS field56940
    , max(case when flatten_name = 'field57396' then flatten_value:defaultAttributes:Label::STRING end) AS field57396
    , max(case when flatten_name = 'field56942' then flatten_value:defaultAttributes:Label::STRING end) AS field56942
    , max(case when flatten_name = 'field57608' then flatten_value::VARIANT end) AS field57608
    , max(case when flatten_name = 'field58722' then flatten_value::VARIANT end) AS field58722
    , max(case when flatten_name = 'field57204' then flatten_value:defaultAttributes:Label::STRING end) AS field57204
    , max(case when flatten_name = 'field57197' then flatten_value:defaultAttributes:Label::STRING end) AS field57197
    , max(case when flatten_name = 'field30649' then flatten_value::VARIANT end) AS field30649
    , max(case when flatten_name = 'field28067' then flatten_value::STRING end) AS field28067
    , max(case when flatten_name = 'field61655' then flatten_value:defaultAttributes:Label::STRING end) AS field61655
    , max(case when flatten_name = 'field30330' then flatten_value::VARIANT end) AS field30330
    , max(case when flatten_name = 'acceptdeclinedate' then flatten_value::TIMESTAMP end) AS acceptdeclinedate
    , max(case when flatten_name = 'field71087' then flatten_value:defaultAttributes:Label::STRING end) AS field71087
    , max(case when flatten_name = 'field57304' then flatten_value::NUMBER end) AS field57304
    , max(case when flatten_name = 'associatedprofile' then flatten_value:id::INTEGER end) AS associatedprofile
    , max(case when flatten_name = 'field57145' then flatten_value:defaultAttributes:Label::STRING end) AS field57145
    , max(case when flatten_name = 'field57147' then flatten_value::STRING end) AS field57147
    , max(case when flatten_name = 'field57286' then flatten_value::STRING end) AS field57286
    , max(case when flatten_name = 'field31717' then flatten_value:defaultAttributes:Label::STRING end) AS field31717
    , max(case when flatten_name = 'field31567' then flatten_value:defaultAttributes:Label::STRING end) AS field31567
    , max(case when flatten_name = 'field31117' then flatten_value::STRING end) AS field31117
    , max(case when flatten_name = 'proposedstartdate' then flatten_value::TIMESTAMP end) AS proposedstartdate
    , max(case when flatten_name = 'field62705' then flatten_value::STRING end) AS field62705
    , max(case when flatten_name = 'field69194' then flatten_value:defaultAttributes:Label::STRING end) AS field69194
    , max(case when flatten_name = 'counteroffer' then flatten_value::VARIANT end) AS counteroffer
    , max(case when flatten_name = 'field31850' then flatten_value:defaultAttributes:Label::STRING end) AS field31850
    , max(case when flatten_name = 'field59293' then flatten_value:defaultAttributes:Label::STRING end) AS field59293
    , max(case when flatten_name = 'field31530' then flatten_value::STRING end) AS field31530
    , max(case when flatten_name = 'field62125' then flatten_value::STRING end) AS field62125
    , max(case when flatten_name = 'status' then flatten_value:defaultAttributes:Label::STRING end) AS status
    , max(case when flatten_name = 'source' then flatten_value::STRING end) AS source
    , max(case when flatten_name = 'field28017' then flatten_value:defaultAttributes:Label::STRING end) AS field28017
    , max(case when flatten_name = 'field31528' then flatten_value:defaultAttributes:Label::STRING end) AS field31528
    , max(case when flatten_name = 'field31601' then flatten_value::STRING end) AS field31601
    , max(case when flatten_name = 'field69127' then flatten_value:defaultAttributes:Label::STRING end) AS field69127
    , max(case when flatten_name = 'field62706' then flatten_value:defaultAttributes:Label::STRING end) AS field62706
    , max(case when flatten_name = 'field57200' then flatten_value::TIMESTAMP end) AS field57200
    , max(case when flatten_name = 'field69188' then flatten_value::VARIANT end) AS field69188
    , max(case when flatten_name = 'field58265' then flatten_value:defaultAttributes:Label::STRING end) AS field58265
    , max(case when flatten_name = 'field69189' then flatten_value:defaultAttributes:Label::STRING end) AS field69189
    , max(case when flatten_name = 'createddate' then flatten_value::TIMESTAMP end) AS createddate
    , max(case when flatten_name = 'field57209' then flatten_value:defaultAttributes:Label::STRING end) AS field57209
    , max(case when flatten_name = 'field31531' then flatten_value:defaultAttributes:Label::STRING end) AS field31531
    , max(case when flatten_name = 'field31762' then flatten_value:defaultAttributes:Label::STRING end) AS field31762
    , max(case when flatten_name = 'field30316' then flatten_value:defaultAttributes:Label::STRING end) AS field30316
    , max(case when flatten_name = 'field29363' then flatten_value::VARIANT end) AS field29363
    , max(case when flatten_name = 'field29365' then flatten_value::STRING end) AS field29365
    , max(case when flatten_name = 'field58187' then flatten_value::NUMBER end) AS field58187
    , max(case when flatten_name = 'field56513' then flatten_value::NUMBER end) AS field56513
    , max(case when flatten_name = 'field31159' then flatten_value:defaultAttributes:Label::STRING end) AS field31159
    , max(case when flatten_name = 'bonus' then flatten_value:defaultAttributes:Label::STRING end) AS bonus
    , max(case when flatten_name = 'field31533' then flatten_value::STRING end) AS field31533
    , max(case when flatten_name = 'field56871' then flatten_value::NUMBER end) AS field56871
    , max(case when flatten_name = 'sourcechannel' then flatten_value::STRING end) AS sourcechannel
    , max(case when flatten_name = 'field57207' then flatten_value:defaultAttributes:Label::STRING end) AS field57207
    , max(case when flatten_name = 'field57194' then flatten_value::TIMESTAMP end) AS field57194
    , max(case when flatten_name = 'field31527' then flatten_value:id::INTEGER end) AS field31527
    , max(case when flatten_name = 'field28064' then flatten_value:defaultAttributes:Label::STRING end) AS field28064
    , max(case when flatten_name = 'field31607' then flatten_value::STRING end) AS field31607
    , max(case when flatten_name = 'field69937' then flatten_value:defaultAttributes:Label::STRING end) AS field69937
    , max(case when flatten_name = 'field31138' then flatten_value::STRING end) AS field31138
    , max(case when flatten_name = 'field29261' then flatten_value:defaultAttributes:Label::STRING end) AS field29261
    , max(case when flatten_name = 'field31853' then flatten_value:defaultAttributes:Label::STRING end) AS field31853
    , max(case when flatten_name = 'field59706' then flatten_value::TIMESTAMP end) AS field59706
    , max(case when flatten_name = 'sourceorigin' then flatten_value:defaultAttributes:Label::STRING end) AS sourceorigin
    , max(case when flatten_name = 'portal' then flatten_value::STRING end) AS portal
    , max(case when flatten_name = 'sourcedevice' then flatten_value::STRING end) AS sourcedevice
    , max(case when flatten_name = 'jibesessionid' then flatten_value::STRING end) AS jibesessionid
    , max(case when flatten_name = 'field57148' then flatten_value::STRING end) AS field57148
    , max(case when flatten_name = 'field30323' then flatten_value:defaultAttributes:Label::STRING end) AS field30323
    , max(case when flatten_name = 'field29361' then flatten_value:id::INTEGER end) AS field29361
    , max(case when flatten_name = 'field72013' then flatten_value::NUMBER end) AS field72013
    , max(case when flatten_name = 'field29318' then flatten_value:defaultAttributes:Label::STRING end) AS field29318
    , max(case when flatten_name = 'field31707' then flatten_value:defaultAttributes:Label::STRING end) AS field31707
    , max(case when flatten_name = 'field31606' then flatten_value:defaultAttributes:Label::STRING end) AS field31606
    , max(case when flatten_name = 'field56868' then flatten_value::NUMBER end) AS field56868
    , max(case when flatten_name = 'field28025' then flatten_value:id::INTEGER end) AS field28025
    , max(case when flatten_name = 'offerexpiration' then flatten_value::TIMESTAMP end) AS offerexpiration
    , max(case when flatten_name = 'offeramount' then flatten_value::VARIANT end) AS offeramount
    , max(case when flatten_name = 'field69190' then flatten_value:defaultAttributes:Label::STRING end) AS field69190
    , max(case when flatten_name = 'field31539' then flatten_value::STRING end) AS field31539
    , max(case when flatten_name = 'field63660' then flatten_value::STRING end) AS field63660
    , max(case when flatten_name = 'field57211' then flatten_value:defaultAttributes:Label::STRING end) AS field57211
    , max(case when flatten_name = 'field31534' then flatten_value:defaultAttributes:Label::STRING end) AS field31534
    , max(case when flatten_name = 'field31604' then flatten_value::VARIANT end) AS field31604
    , max(case when flatten_name = 'field31120' then flatten_value:defaultAttributes:Label::STRING end) AS field31120
    , max(case when flatten_name = 'field61347' then flatten_value::STRING end) AS field61347
    , max(case when flatten_name = 'field31562' then flatten_value:defaultAttributes:Label::STRING end) AS field31562
    , max(case when flatten_name = 'field31711' then flatten_value::TIMESTAMP end) AS field31711
    , max(case when flatten_name = 'field31526' then flatten_value:defaultAttributes:Label::STRING end) AS field31526
    , max(case when flatten_name = 'field71676' then flatten_value:defaultAttributes:Label::STRING end) AS field71676
    , max(case when flatten_name = 'relocationamount' then flatten_value:defaultAttributes:Label::STRING end) AS relocationamount
    , max(case when flatten_name = 'field31190' then flatten_value::NUMBER end) AS field31190
    , max(case when flatten_name = 'updatedby' then flatten_value:id::INTEGER end) AS updatedby
    , max(case when flatten_name = 'field57198' then flatten_value::STRING end) AS field57198
    , max(case when flatten_name = 'field61071' then flatten_value::STRING end) AS field61071
    , max(case when flatten_name = 'field58039' then flatten_value:id::INTEGER end) AS field58039
    , max(case when flatten_name = 'field28063' then flatten_value:defaultAttributes:Label::STRING end) AS field28063
    , max(case when flatten_name = 'field58092' then flatten_value::VARIANT end) AS field58092
    , max(case when flatten_name = 'field31603' then flatten_value::STRING end) AS field31603
    , max(case when flatten_name = 'field28065' then flatten_value:defaultAttributes:Label::STRING end) AS field28065
    , max(case when flatten_name = 'field31130' then flatten_value:defaultAttributes:Label::STRING end) AS field31130
    , max(case when flatten_name = 'field31578' then flatten_value::VARIANT end) AS field31578
    , max(case when flatten_name = 'field62704' then flatten_value::STRING end) AS field62704
    , max(case when flatten_name = 'field56941' then flatten_value::STRING end) AS field56941
    , max(case when flatten_name = 'field29265' then flatten_value:defaultAttributes:Label::STRING end) AS field29265
    , max(case when flatten_name = 'field57203' then flatten_value::STRING end) AS field57203
    , max(case when flatten_name = 'field31196' then flatten_value:defaultAttributes:Label::STRING end) AS field31196
    , max(case when flatten_name = 'field31158' then flatten_value:defaultAttributes:Label::STRING end) AS field31158
    , max(case when flatten_name = 'field58192' then flatten_value:defaultAttributes:Label::STRING end) AS field58192
    , max(case when flatten_name = 'field29369' then flatten_value:defaultAttributes:Label::STRING end) AS field29369
    , max(case when flatten_name = 'field31566' then flatten_value::TIMESTAMP end) AS field31566
    , max(case when flatten_name = 'field69186' then flatten_value::STRING end) AS field69186
    , max(case when flatten_name = 'field31580' then flatten_value::STRING end) AS field31580
    , max(case when flatten_name = 'field31602' then flatten_value:defaultAttributes:Label::STRING end) AS field31602
    , max(case when flatten_name = 'field31540' then flatten_value:defaultAttributes:Label::STRING end) AS field31540
    , max(case when flatten_name = 'field57212' then flatten_value:defaultAttributes:Label::STRING end) AS field57212
    , max(case when flatten_name = 'field57196' then flatten_value::STRING end) AS field57196
    , max(case when flatten_name = 'field57205' then flatten_value::TIMESTAMP end) AS field57205
    , max(case when flatten_name = 'updateddate' then flatten_value::TIMESTAMP end) AS updateddate
    , max(case when flatten_name = 'field57509' then flatten_value::VARIANT end) AS field57509
    , max(case when flatten_name = 'field69163' then flatten_value:defaultAttributes:Label::STRING end) AS field69163
    , max(case when flatten_name = 'field31134' then flatten_value:defaultAttributes:Label::STRING end) AS field31134
    , max(case when flatten_name = 'field62007' then flatten_value::STRING end) AS field62007
    , max(case when flatten_name = 'field31599' then flatten_value:id::INTEGER end) AS field31599
    , max(case when flatten_name = 'field31155' then flatten_value:defaultAttributes:Label::STRING end) AS field31155
    , max(case when flatten_name = 'field31187' then flatten_value::STRING end) AS field31187
    , max(case when flatten_name = 'field57908' then flatten_value:defaultAttributes:Label::STRING end) AS field57908
    , max(case when flatten_name = 'field57195' then flatten_value::TIMESTAMP end) AS field57195
    , max(case when flatten_name = 'field57210' then flatten_value:defaultAttributes:Label::STRING end) AS field57210
    , max(case when flatten_name = 'field63765' then flatten_value:defaultAttributes:Label::STRING end) AS field63765
    , max(case when flatten_name = 'field28066' then flatten_value::TIMESTAMP end) AS field28066
    , max(case when flatten_name = 'field32036' then flatten_value:id::INTEGER end) AS field32036
    , max(case when flatten_name = 'field28062' then flatten_value::STRING end) AS field28062
    , max(case when flatten_name = 'field57536' then flatten_value::VARIANT end) AS field57536
    , max(case when flatten_name = 'field57214' then flatten_value::TIMESTAMP end) AS field57214
    , max(case when flatten_name = 'field69187' then flatten_value::STRING end) AS field69187
    , max(case when flatten_name = 'field29303' then flatten_value:defaultAttributes:Label::STRING end) AS field29303
    , max(case when flatten_name = 'field29304' then flatten_value:defaultAttributes:Label::STRING end) AS field29304
    , max(case when flatten_name = 'field56945' then flatten_value:defaultAttributes:Label::STRING end) AS field56945
    , max(case when flatten_name = 'field57609' then flatten_value::TIMESTAMP end) AS field57609
    , max(case when flatten_name = 'field57199' then flatten_value:defaultAttributes:Label::STRING end) AS field57199
    , max(case when flatten_name = 'field57430' then flatten_value:id::INTEGER end) AS field57430
    , max(case when flatten_name = 'field31600' then flatten_value::NUMBER end) AS field31600
    , max(case when flatten_name = 'field31490' then flatten_value:defaultAttributes:Label::STRING end) AS field31490
    , max(case when flatten_name = 'field31537' then flatten_value::STRING end) AS field31537
    , max(case when flatten_name = 'field56943' then flatten_value:defaultAttributes:Label::STRING end) AS field56943
    , max(case when flatten_name = 'field56944' then flatten_value:defaultAttributes:Label::STRING end) AS field56944
    , max(case when flatten_name = 'field31183' then flatten_value::NUMBER end) AS field31183
    , max(case when flatten_name = 'field57208' then flatten_value::STRING end) AS field57208
    , max(case when flatten_name = 'field63728' then flatten_value:defaultAttributes:Label::STRING end) AS field63728
    , max(case when flatten_name = 'jibeuserid' then flatten_value::STRING end) AS jibeuserid
    , max(case when flatten_name = 'field31563' then flatten_value:id::INTEGER end) AS field31563
    , max(case when flatten_name = 'field31535' then flatten_value:defaultAttributes:Label::STRING end) AS field31535
    , max(case when flatten_name = 'field28015' then flatten_value::STRING end) AS field28015
    , max(case when flatten_name = 'field63931' then flatten_value:defaultAttributes:Label::STRING end) AS field63931
    , max(case when flatten_name = 'field31851' then flatten_value:defaultAttributes:Label::STRING end) AS field31851
    , max(case when flatten_name = 'field31704' then flatten_value::TIMESTAMP end) AS field31704
    , max(case when flatten_name = 'field61436' then flatten_value::STRING end) AS field61436
    , max(case when flatten_name = 'field57542' then flatten_value::TIMESTAMP end) AS field57542
    , max(case when flatten_name = 'field57202' then flatten_value:defaultAttributes:Label::STRING end) AS field57202
    , max(case when flatten_name = 'field31564' then flatten_value::STRING end) AS field31564
    , max(case when flatten_name = 'field31761' then flatten_value:defaultAttributes:Label::STRING end) AS field31761
    , max(case when flatten_name = 'field31483' then flatten_value:defaultAttributes:Label::STRING end) AS field31483
    , max(case when flatten_name = 'field29331' then flatten_value::STRING end) AS field29331
    , max(case when flatten_name = 'sourceemail' then flatten_value::STRING end) AS sourceemail
    , max(case when flatten_name = 'field60871' then flatten_value::STRING end) AS field60871
    , max(case when flatten_name = 'field29360' then flatten_value:id::INTEGER end) AS field29360
    , max(case when flatten_name = 'field31849' then flatten_value:defaultAttributes:Label::STRING end) AS field31849
    from source_flatten_data
    group by deleted
    , id
    , lastUpdated
)
, final as
(SELECT d.*
, typeof(HASH(d.*)) as row_hash
, current_timestamp() as etl_updated_timestamp
 from source_data d)
SELECT src.* from final src

{% if is_incremental() %}
LEFT OUTER JOIN {{ this }} tgt
ON src.id = tgt.id and src.row_hash = tgt.row_hash
WHERE tgt.id IS NULL
{% endif %}