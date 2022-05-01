"""
Test cases for SchemaBuilder

"""
from typing import List

from app.utils.create_schema_test import SchemaBuilder


def test_all_models_selected(mocker):
    mocker.patch(
        'app.utils.create_schema_test.SchemaBuilder.write_yaml',  # Do not write in disk for this test
        return_value=None
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
