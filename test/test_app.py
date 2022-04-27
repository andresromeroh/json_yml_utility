import os
import pathlib
from shutil import rmtree
from typing import List

import pytest
from ruamel import yaml

from app.create_schema_test import SchemaBuilder


@pytest.fixture(autouse=True)
def teardown():
    yield
    # Deletes directory created by cli
    rmtree(f"{pytest.context_path}/model")


def test_schema_builder_one_model():
    context_path = get_context_path()
    expected_count_output = 1
    expected_name_output = 'COMPANY.yml'
    project_dir = '/app'
    model = 'company'
    update = True
    builder = SchemaBuilder(project_dir=project_dir, model_selected=model, update=update)
    builder.start()
    # Test only one file created
    files = files_generated(f"{context_path}/model")
    assert len(files) == expected_count_output
    # Test the output name
    assert expected_name_output in files
    # Test the content of the yml, using the lib
    yml = yaml.YAML()
    company_yml = yml.load(pathlib.Path(f"{context_path}/model/COMPANY.yml"))
    model = company_yml["models"][0]
    assert model['name'] == 'COMPANY'
    assert model['description'] == 'RAW TABLE FOR ICIMS COMPANY'
    assert model['dbt_utils.recency'] is not None
    assert model['dbt_expectations.expect_column_distinct_count_to_equal_other_table'] is not None
    assert model['meta'] is not None
    assert model['meta']['slo'] is not None
    assert model['meta']['ownership'] is not None
    assert model['meta']['detailed_description'] is not None
    assert model['meta']['build_requirements'] is not None
    assert model['meta']['origination'] is not None
    assert model['meta']['filters_and_limitations'] is not None
    assert model['meta']['data_access'] is not None
    assert model['meta']['data_quality'] is not None
    assert model['columns'] is not None

    for column in model['columns']:
        assert column['name'] in ['RAW_DATA', 'RAW_INSERTED_TIMESTAMP', 'RAW_FILENAME']
        assert column['description'] is None
        assert column['tests'] is not None
        assert column['meta'] is not None


def files_generated(path: str) -> List[str]:
    return os.listdir(path)


def get_context_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
