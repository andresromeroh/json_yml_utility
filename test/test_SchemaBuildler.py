"""
Test cases for SchemaBuilder

"""
import json
import os
from typing import List, Dict

from app.utils.create_schema_test import SchemaBuilder


def test_all_models_selected(mocker):
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.write_yaml',  # Do not write in disk for this test
        return_value=None
    )
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.read_json',
        return_value=read_test_catalog()
    )
    project_dir = 'app/dbt_ingest'
    model = None
    update = True
    builder = SchemaBuilder(
        project_dir=project_dir, model_selected=model, update=update
    )
    builder.start()
    output_models: List = builder.models
    assert output_models


def test_single_model_selected(mocker):
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.write_yaml',  # Do not write in disk for this test
        return_value=None
    )
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.read_json',
        return_value=read_test_catalog()
    )
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


def test_single_model_selected_schema(mocker):
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.write_yaml',  # Do not write in disk for this test
        return_value=None
    )
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.read_json',
        return_value=read_test_catalog()
    )
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
        assert column['name'] in ['RAW_DATA', 'RAW_INSERTED_TIMESTAMP', 'RAW_FILENAME']
        assert column['description'] is None
        assert column['tests'] is not None
        assert column['meta'] is not None


def read_test_catalog() -> Dict:
    path = os.path.abspath(f"{os.path.dirname(__file__)}/resources/target/catalog.json")
    with open(path) as json_file:
        return json.load(json_file)
