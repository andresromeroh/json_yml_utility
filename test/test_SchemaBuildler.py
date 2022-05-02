"""
Test cases for SchemaBuilder

"""
import json
import os
from typing import List, Dict

import pytest

from app.utils.create_schema_test import SchemaBuilder


@pytest.fixture()
def mocked_write_yaml(mocker):  # mocker is pytest-mock fixture
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.write_yaml',  # Do not write in disk for this test
        return_value=None
    )


@pytest.fixture()
def mocked_read_json(mocker):  # mocker is pytest-mock fixture
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.read_json',
        return_value=read_test_catalog()
    )


def test_all_models_selected(mocked_write_yaml, mocked_read_json):
    """
    Testing all models generated
    """
    project_dir = 'app/dbt_ingest'
    model = None
    update = True
    builder = SchemaBuilder(
        project_dir=project_dir, model_selected=model, update=update
    )
    builder.start()
    output_models: List = builder.models
    model_names = ['CONNECTEVENTWORKFLOWSOURCE', 'PERSON', 'ROOM', 'CONNECTEVENTWORKFLOW', 'SUBMITTAL', 'TALENTPOOL',
                   'CONNECTEVENT', 'COMPANY', 'SOURCEWORKFLOW', 'WORKDAY_TRANSACTIONS', 'WORKDAY_SUPPLIERS',
                   'WORKDAY_JOB_PROFILES',
                   'WORKDAY_CUSTOM_ORGANIZATIONS', 'WORKDAY_COMPANIES', 'FORM_BUILDER_FORM_HISTORY',
                   'CKAPP_FUNCTION_HISTORY', 'FEEDBACK_EMPLOYEEPTLSNAPSHOT_HISTORY', 'CKAPP_RUBRIC_HISTORY',
                   'CKAPP_ROLE_HISTORY',
                   'CKAPP_ORGANIZATION_HISTORY', 'CKAPP_LEVEL_HISTORY', 'CKAPP_DIMENSION_HISTORY',
                   'FEEDBACK_ROUND_HISTORY', 'WORKDAY_COST_CENTERS',
                   'WORKDAY_LOCATIONS', 'WORKDAY_LOCATIONS_HIERARCHY', 'WORKDAY_SUPERVISORY_ORGANIZATIONS',
                   'FEEDBACK_EMPLOYEE_PTL_SNAPSHOT', 'FEEDBACK_FORM_BUILDER_FORM', 'FEEDBACK_FUNCTION',
                   'FEEDBACK_LEVEL', 'FEEDBACK_ORGANIZATION', 'FEEDBACK_ROLE', 'FEEDBACK_ROUND', 'FEEDBACK_RUBRIC',
                   'WORKDAY_JOB_PROFILES',
                   'FEEDBACK_DIMENSION']
    assert output_models
    assert [model['models'][0]['name'] for model in output_models] == model_names


def test_single_model_selected(mocked_write_yaml, mocked_read_json):
    """
    Testing only one selected model: COMPANY
    """
    project_dir = 'app/dbt_ingest'
    model = "company"
    update = True
    builder = SchemaBuilder(
        project_dir=project_dir, model_selected=model, update=update
    )
    builder.start()
    output_models: List = builder.models
    assert output_models
    assert len(output_models) == 1
    model = output_models[0]
    model_selected = model['models'][0]
    assert model_selected['name'] == 'COMPANY'
    assert model_selected['description'] == 'RAW TABLE FOR ICIMS COMPANY'


def test_single_model_selected_schema(mocked_write_yaml, mocked_read_json):
    """
    Testing only one selected model:FEEDBACK_LEVEL and schema validation
    """
    project_dir = 'app/dbt_ingest'
    model = "FEEDBACK_LEVEL"
    update = True
    builder = SchemaBuilder(
        project_dir=project_dir, model_selected=model, update=update
    )
    builder.start()
    output_models: List = builder.models
    assert output_models
    assert len(output_models) == 1
    model = output_models[0]
    model_selected = model['models'][0]
    assert model_selected['name'] == 'FEEDBACK_LEVEL'
    assert model_selected['description'] is None

    for test in model_selected['test']:
        assert test['dbt_utils.recency']
        assert test['dbt_expectations.expect_column_distinct_count_to_equal_other_table']

    assert model_selected['meta']
    assert model_selected['meta']['slo']
    assert model_selected['meta']['ownership']
    assert model_selected['meta']['detailed_description']
    assert model_selected['meta']['build_requirements']
    assert model_selected['meta']['origination']
    assert model_selected['meta']['filters_and_limitations']
    assert model_selected['meta']['data_access']
    assert model_selected['meta']['data_quality']

    for column in model_selected['columns']:
        assert column['name'] in ['LEVEL_KEY', 'ID', 'NAME', 'DESCRIPTION', 'RUBRIC_ID', 'SEQ', 'IS_SPECIFIED',
                                  'ETL_UPDATED_TIMESTAMP', 'IS_DELETED_IN_SOURCE', 'TIMESTAMP_EFFECTIVE', 'ROW_HASH']
        assert column['description'] is None
        assert column['tests'] is not None
        assert column['meta'] is not None


def read_test_catalog() -> Dict:
    path = os.path.abspath(f"{os.path.dirname(__file__)}/resources/target/catalog.json")
    with open(path) as json_file:
        return json.load(json_file)
