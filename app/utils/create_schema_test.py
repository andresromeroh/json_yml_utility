#!/usr/bin/env python
# -*- coding: utf-8 -*-

import enum
import json
import logging
import os
from os.path import exists
from typing import Dict

import click
from ruamel import yaml
from ruamel.yaml.comments import (
    CommentedMap as OrderedDict,
    CommentedSeq as OrderedList,
)

INDENTATION: int = 2

SOURCES: Dict = {
    "MODELS_METADATA": "catalog.json",
    "MODELS_PATH": "manifest.json",
}


class Literal(enum.Enum):
    MODEL = "model"
    NODES = "nodes"
    SOURCE = "source"
    SOURCES = "sources"
    MODELS_MANIFEST = "manifest.json"
    MODELS_CATALOG = "catalog.json"


logging.basicConfig(level=logging.INFO)


class SchemaBuilder(object):
    """Builds schema test yml files for models within a dbt project.

    Note that this code is dependent upon a manifest.json file within the project's target directory.
    This file is not configuration managed and is updated and dbt models are compiled and run.  Ensure
    that you compile the entirity of the dbt project you intend to generate schema tests for prior to
    executing this script.
    """

    def __init__(self, project_dir, model_selected=None, update=False, version=2):
        self.project_dir = (project_dir if not project_dir.endswith(
            "/") else project_dir[:-1])
        self.model_selected = model_selected
        self.model_sources = []
        self.models = []
        self.version = version
        self.update = update
        self.destination_path = ""
        self.base_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                      '../..'))  # Project Root abs path, assuming target is two folders deep
        self.root_directory = 'app'
        self.file_paths = self.build_models_paths()

    def start(self):
        """Generate the schema test yaml files."""
        try:
            self.model_sources = self.get_models_from_catalog(self.model_selected)
        except FileNotFoundError as e:
            logging.error("Manifest.json file not found!")

        self.build_yml_objs()
        self.process_models()

    def build_models_paths(self) -> Dict:
        paths = {}
        for path, dirs, files in os.walk(os.path.join(self.base_path, self.root_directory)):
            for file in files:
                filename_with_no_extension: str = file.split(".")[0]
                paths[filename_with_no_extension.lower()] = os.path.join(path, file)
        return paths

    def get_models_from_manifest(self, model_selected=None):
        """
            Parse the manifest.json file and return only the models selected (or all models if model_selected is None).
            @DEPRECATED, USE SchemaBuilder.build_models_paths()
        """
        with open(
                os.path.join(self.base_path, self.project_dir, "target", Literal.MODELS_MANIFEST.value)
        ) as json_file:
            manifest_nodes = json.load(json_file)[Literal.NODES.value]
            return [
                content
                for name, content in manifest_nodes.items()
                if name.startswith(Literal.MODEL.value)
                   and ((model_selected is None) or (content["name"] == model_selected))
                   and os.path.split(content["root_path"])[-1] == os.path.split(self.project_dir)[-1]
            ]

    def get_models_from_catalog(self, model_selected=None):
        """Parse the CATALOG file and return only the selected model(s), all models by default."""
        lower_model_selected = model_selected.lower() if model_selected else None
        file_to_open = os.path.join(self.base_path, self.project_dir, "target", Literal.MODELS_CATALOG.value)
        with open(file_to_open) as json_file:
            raw_load = json.load(json_file)
            input_sources = {**raw_load[Literal.SOURCES.value], **raw_load[Literal.NODES.value]}
            models_selected = [
                content
                for name, content in input_sources.items()
                if (name.startswith(Literal.SOURCE.value) or name.startswith(Literal.MODEL.value)) and (
                        (not lower_model_selected) or (content["metadata"]["name"].lower() == lower_model_selected))
            ]
            model_names = [model["metadata"]["name"] for model in models_selected]
            logging.info(f"...Processing {len(models_selected)} models...")
            if models_selected:
                logging.info(f"{model_names}")

            return models_selected

    def build_yml_objs(self):
        """Appending the create yml objs from the model sources to process"""

        for model in self.model_sources:
            try:
                yml_obj = self._build_yml_model_obj(model)
                self.models.append(yml_obj)
            except Exception as e:
                logging.info(f"Error building model object with error: '{e}'")

    def find_model_original_path(self, dbt_model_name):
        """Find the model original directory paths from reading the project tree"""
        path_for_model = self.file_paths.get(dbt_model_name.lower()) if dbt_model_name else None
        if path_for_model:
            self.destination_path = path_for_model
        return self.destination_path

    def _build_yml_model_obj(self, model_with_columns):
        """Generates the YML structure obj the dbt model"""
        try:
            result = OrderedDict(
                {
                    "version": self.version,
                    "models": [
                        OrderedDict(
                            {
                                "name": model_with_columns["metadata"]["name"],
                                "description": model_with_columns["metadata"]["comment"],
                                "original_file_path": self.find_model_original_path(
                                    model_with_columns["unique_id"].split(".")[-1]),
                                "test": [
                                    OrderedDict(
                                        {
                                            "dbt_utils.recency": OrderedDict(
                                                {
                                                    "datepart": None,
                                                    "field": None,
                                                    "interval": 1,
                                                    "tags": "timeliness",
                                                    "severity": "warn",
                                                }
                                            ),
                                            "dbt_expectations.expect_column_distinct_count_to_equal_other_table": OrderedDict(
                                                {
                                                    "column_name": None,
                                                    "row_condition": None,
                                                    "compare_model": None,
                                                    "compare_column_name": None,
                                                    "compare_row_condition": True,
                                                    "tags": "completeness",
                                                    "severity": "warn",
                                                }
                                            ),
                                        }
                                    )
                                ],
                                "meta": OrderedDict(
                                    {
                                        "slo": OrderedDict(
                                            {
                                                "included": True,
                                                "delivery_time_ct": None,
                                                "offset": None,
                                                "is_static": None,
                                                "contact": None,
                                                "tag": None,
                                            }
                                        ),
                                        "ownership": OrderedDict(
                                            {
                                                "team_id": None,
                                                "ddo_delegate": None,
                                                "ddo_delegate_ldap": None,
                                                "subject_matter_expert": None,
                                                "subject_matter_expert_ldap": None,
                                                "approver": None,
                                                "approver_ldap": None,
                                                "approver_role": None,
                                            }
                                        ),
                                        "detailed_description": OrderedDict(
                                            {
                                                "dataset_granularity": None,
                                                "classification": None,
                                                "classification_rationale": None,
                                                "sensitive": None,
                                                "is_degraded": None,
                                                "comments": None,
                                            }
                                        ),
                                        "build_requirements": OrderedDict(
                                            {
                                                "link_to_builder_code": None,
                                                "link_to_orchestration_tool": None,
                                                "link_to_design_document": None,
                                            }
                                        ),
                                        "origination": OrderedDict(
                                            {
                                                "refresh_cadence": None,
                                                "refresh_schedule": None,
                                            }
                                        ),
                                        "filters_and_limitations": OrderedDict(
                                            {
                                                "standard_filters_at_consumption": None,
                                                "general_limitations": None,
                                                "field_discontinuities_irregularities": None,
                                                "missing_data": None,
                                            }
                                        ),
                                        "data_access": OrderedDict(
                                            {
                                                "access_controls": None,
                                                "access_controls_review": None,
                                            }
                                        ),
                                        "data_quality": OrderedDict(
                                            {"health_check": None}
                                        ),
                                    }
                                ),
                            }
                        )
                    ],
                }
            )
            cols = self._create_columns(model_with_columns)
            if cols:
                result["models"][0]["columns"] = cols
            logging.debug(f"result {result}")
        except Exception as error:
            logging.info(f"Error building obj for {error}")
        return result

    def _create_columns(self, model):
        """Generates the YML columns and data types of the given dbt model"""
        try:
            model_columns = [column for column in model["columns"].values()]
            return OrderedList(
                [
                    OrderedDict(
                        {
                            "name": column["name"],
                            "description": None,
                            "tests": OrderedList(
                                [
                                    OrderedDict(
                                        {
                                            "not_null": OrderedDict(
                                                {"tags": "validity",
                                                 "severity": "warn"}
                                            )
                                        }
                                    )
                                ]
                            ),
                            "meta": OrderedDict(
                                {
                                    "type": column["type"],
                                    "privacy_classification": None,
                                    "ldm_model": None,
                                    "ldm_attribute": None,
                                    "datasource": None,
                                    "field": None,
                                    "comments": column["comment"],
                                }
                            ),
                        }
                    )
                    for column in model_columns
                    if model["columns"]
                ]
            )

        except Exception as error:
            logging.info(f"Error adding the columns of model {error} {model}")

    def process_models(self):
        """Iterate over the models to be written on the output yaml."""
        for dbt_model in self.models:
            is_processed = False
            try:
                dbt_model_name = dbt_model['models'][0]['name']
                is_processed = self.write_yaml(dbt_model, dbt_model_name)
            except Exception as error:
                logging.error(f"Error processing {dbt_model_name} | '{error}'")
            if is_processed:
                logging.info(f"Successfully processed {dbt_model_name}")
            else:
                logging.warning(f"Skipped model {dbt_model_name}. Add -u flag to update all the existing models.")

    def write_yaml(self, dbt_model, dbt_model_name):
        """Write the yaml files for schema testing."""
        logging.info(f"...Writing {dbt_model_name}")
        logging.debug(f"...obj {dbt_model}")
        yml = yaml.YAML()
        yml.indent(mapping=2, sequence=4, offset=2)
        self.destination_path = self.find_model_original_path(dbt_model_name)
        file_to_write = f"{self.destination_path}"[:-3] + "yml"
        if exists(file_to_write) and not self.update:
            return False
        else:
            with open(file_to_write, "w+") as f:
                dbt_model["version"] = 2
                yml.dump(dbt_model, f)
            return True


@click.command(
    help="This script generates schema test yml files for models within a dbt project"
)
@click.option(
    "--project-dir",
    "-pd",
    default="/app/dbt_transform",
    help=(
            "The path to the dbt project for which to generate schema tests. \
            The default is dbt_transform."
    ),
)
@click.option(
    "--model",
    "-m",
    default=None,
    help=(
            "The name of the model to generate schema tests for.  If not passed \
            schema tests will be generated for all models within the project."
    ),
)
@click.option(
    "--update",
    "-u",
    is_flag=True,
    help="A flag that determines whether or not the yml files will be updated.",
)
def main(project_dir, model, update):
    builder = SchemaBuilder(
        project_dir=project_dir, model_selected=model, update=update
    )
    builder.start()
    logging.info("---- Schema Builder Utility Finished! ----\n")


if __name__ == "__main__":
    main()
