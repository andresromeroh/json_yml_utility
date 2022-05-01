# JSON YML UTILITY CLI

Read from JSON file, generate schema_test.yml from SQL models based on their metada

## How to run it
```shell
python -pd /app -m company -u

# pd: project directory -> default '/app'
# m: model -> default None
# u: A flag that determines whether or not the yml files will be updated-> default None
```

## Running the tests
```shell
pytest 
pytest --cov  #to generate coverage report
```


## Sample Input
Input is located at ``` /sources_truth ```
```json
{
  "metadata": 1,
  "nodes": 1,
  "sources": {
    "source.hrdm_ingest.icims_raw.company": {
      "metadata": {
        "type": "BASE TABLE",
        "schema": "ICIMS",
        "name": "COMPANY",
        "database": "HR_RAW_QA",
        "comment": "RAW TABLE FOR ICIMS COMPANY",
        "owner": "_DB_HR_RAW_QA_ICIMS_OWN"
      },
      "columns": {
        "RAW_DATA": {
            "type": "VARIANT",
            "index": 1,
            "name": "RAW_DATA",
            "comment": null
        },
        "RAW_INSERTED_TIMESTAMP": {
            "type": "TIMESTAMP_TZ",
            "index": 2,
            "name": "RAW_INSERTED_TIMESTAMP",
            "comment": null
        },
        "RAW_FILENAME": {
            "type": "TEXT",
            "index": 3,
            "name": "RAW_FILENAME",
            "comment": null
        }
      },
      "stats": {
        "bytes": {
            "id": "bytes",
            "label": "Approximate Size",
            "value": 2404352.0,
            "include": true,
            "description": "Approximate size of the table as reported by Snowflake"
        },
        "row_count": {
            "id": "row_count",
            "label": "Row Count",
            "value": 14023.0,
            "include": true,
            "description": "An approximate count of rows in this table"
        },
        "last_modified": {
            "id": "last_modified",
            "label": "Last Modified",
            "value": "2022-03-29 13:48UTC",
            "include": true,
            "description": "The timestamp for last update/change"
        },
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": true,
            "include": false,
            "description": "Indicates whether there are statistics for this table"
        }
      },
      "unique_id": "source.hrdm_ingest.icims_raw.company"
    }
  },
  "errors": null
}
```

## Sample Output
Output is located at ``` /app/model/ ```
```yml
{
  version: 2
    models:
      - name: COMPANY
        description: RAW TABLE FOR ICIMS COMPANY
        dbt_utils.recency:
          datepart: day
          field: etl_updated_timestamp
          interval: 1
          tags: timeliness
          severity: warn
        dbt_expectations.expect_column_distinct_count_to_equal_other_table:
          column_name: job_source_id
          row_condition: COLUMN_MODEL <> '-1'
          compare_model: ref('COLUMN_MODEL')
          compare_column_name: COLUMN_MODEL
          compare_row_condition: true
          tags: completeness
          severity: warn
        meta:
          slo:
            included: true
            delivery_time_ct:
            offset:
            is_static:
            contact:
            tag:
          ownership:
            team_id:
            ddo_delegate:
            ddo_delegate_ldap:
            subject_matter_expert:
            subject_matter_expert_ldap:
            approver:
            approver_ldap:
            approver_role:
          detailed_description:
            dataset_granularity:
            classification:
            classification_rationale:
            sensitive:
            is_degraded:
            comments:
          build_requirements:
            link_to_builder_code:
            link_to_orchestration_tool:
            link_to_design_document:
          origination:
            refresh_cadence:
            refresh_schedule:
          filters_and_limitations:
            standard_filters_at_consumption:
            general_limitations:
            field_discontinuities_irregularities:
            missing_data:
          data_access:
            access_controls:
            access_controls_review:
          data_quality:
            health_check:
        columns:
          - name: RAW_DATA
            description:
            tests:
              - not_null:
                  tags: validity
                  severity: warn
            meta:
              type: VARIANT
              privacy_classification:
              ldm_model:
              ldm_attribute:
              datasource:
              field:
              comments: ''
          - name: RAW_INSERTED_TIMESTAMP
            description:
            tests:
              - not_null:
                  tags: validity
                  severity: warn
            meta:
              type: TIMESTAMP_TZ
              privacy_classification:
              ldm_model:
              ldm_attribute:
              datasource:
              field:
              comments: ''
          - name: RAW_FILENAME
            description:
            tests:
              - not_null:
                  tags: validity
                  severity: warn
            meta:
              type: TEXT
              privacy_classification:
              ldm_model:
              ldm_attribute:
              datasource:
              field:
              comments: ''
}
```

--------------------------------
From client's description:
Drive the creation of .yml files using the catalog.json file instead of the manifest.json file (includes columns names and data types).

Ensure for each column the name and data type are populated in addition to defaults with description, tests (not null), etc.
That the user will then manually update later on.

line 76 is the obj --> model_obj = self._build_model_obj(model)  # THIS IS stuff.json
https://github.com/Python-Revolution/json_yml_utility/blob/6026462f3dde1313256561990f797ac8e146eac0/app/create_schema_test.py#L76


<!--
Models not found in catalog.json
    1. feedback_calibration
    2. feedback_dimension_value_feedback_item
    3. feedback_employee_ptl_info
    4. feedback_feedback
    5. feedback_integer_feedback_item
    5. feedback_workday_profile
    6. icims_code_generator
    7. feedback_integer_feedback_item
-->
