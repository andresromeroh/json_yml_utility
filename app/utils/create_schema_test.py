#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import logging
from typing import Dict
from os.path import exists

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
        self.model_paths = []
        self.models = []
        self.version = version
        self.update = update
        self.destination_path = ""

    def start(self):
        """Generate the schema test yaml files."""
        try:
            self.model_paths = self.get_models_from_manifest(self.model_selected)
            self.model_sources = self.get_models_from_catalog(self.model_selected)
        except:
            logging.error("Manifest.json file not found!")

        self.build_yml_objs()
        self.process_models()

    def get_models_from_manifest(self, model_selected=None):
        """Parse the manifest.json file and return only the models selected (or all models if model_selected is None)."""
        with open(
            os.path.join(self.project_dir, "target", SOURCES["MODELS_PATH"])
        ) as json_file:
            manifest_nodes = json.load(json_file)["nodes"]
            return [
                content
                for name, content in manifest_nodes.items()
                if name.startswith("model")
                and ((model_selected is None) or (content["name"] == model_selected))
                and os.path.split(content["root_path"])[-1]
                == os.path.split(self.project_dir)[-1]
            ]

    def get_models_from_catalog(self, model_selected=None):
        """Parse the CATALOG file and return only the selected model(s), all models by default."""
        lower_model_selected = model_selected.lower() if model_selected else None
        file_to_open = os.path.join(
            self.project_dir, "target", SOURCES["MODELS_METADATA"]
        )
        with open(file_to_open) as json_file:
            input_sources = json.load(json_file)["sources"]
            models_selected = [
                content
                for name, content in input_sources.items()
                if name.startswith("source") and ((not lower_model_selected)
                                                  or (content["metadata"]["name"].lower() == lower_model_selected))
            ]

            model_names = [model["metadata"]["name"]
                           for model in models_selected]
            qty_models = len(models_selected)

            logging.info(f"...Processing {qty_models} models...")

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
        """Find the model original directoy paths from manifest.json"""
        try:
            model_original_paths = []

            if not self.destination_path:
                for model_node in self.model_paths:
                    model_original_paths.append(
                        model_node["original_file_path"])

                for path in model_original_paths:
                    if dbt_model_name in path:
                        destination_path = path

                self.destination_path = destination_path
        except Exception as error:
            logging.info(f"Error finding the path of model {error}")
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
                                    model_with_columns["unique_id"].split(".")[3]),
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
        import pdb; pdb.set_trace()

        yml = yaml.YAML()
        yml.indent(mapping=2, sequence=4, offset=2)
        self.destination_path = self.find_model_original_path(dbt_model_name)
        file_to_write = f"{self.project_dir}/{self.destination_path}"[:-3] + "yml"

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
