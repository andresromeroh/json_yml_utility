#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
from os.path import exists
from typing import Dict

import click
from ruamel import yaml
from ruamel.yaml.comments import \
    CommentedMap as OrderedDict, \
    CommentedSeq as OrderedList

INDENTATION = 2

logging.basicConfig(level=logging.INFO)

CONSTANTS: Dict = {
    'INPUT_JSON': 'catalog.json'
}


class SchemaBuilder(object):
    """Builds schema test yml files for models within a dbt project.

    Note that this code is dependent upon a manifest.json file within the project's target directory.
    This file is not configuration managed and is updated and dbt models are compiled and run.  Ensure
    that you compile the entirity of the dbt project you intend to generate schema tests for prior to
    executing this script.
    """

    def __init__(self, project_dir, model_selected=None, update=False, version=2):
        self.project_dir = project_dir if not project_dir.endswith(
            '/') else project_dir[:-1]
        self.model_selected = model_selected
        self.models = []
        self.model_objs = []
        self.version = version
        self.update = update

    def start(self):
        """Generate the schema test yaml files."""
        try:
            logging.info("---- Schema Builder Utility Started! ----")
            self.models = self.get_models_from_manifest(self.model_selected)
        except FileNotFoundError as e:
            logging.error(f"Manifest.json file not found!")
        try:
            self.build_objs()
            self.process_models()
            pass
        except:
            logging.error(f"Error running the utility 'schema test' !")

    def get_models_from_manifest(self, model_selected=None):
        """Parse the manifest.json file and return only the selected model(s), all models by default."""
        file_to_open = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '../sources_truth', CONSTANTS.get('INPUT_JSON')))
        with open(file_to_open) as json_file:  # TODO: absolute PATH, fix next commit to relative
            manifest_nodes = json.load(json_file)["sources"]
            models_selected = [
                content for name, content in manifest_nodes.items()]
            #     if name.startswith('model') and
            #        ((model_selected is None) or (content["metadata"]['name'] == model_selected)) and True
            #       # and os.path.split(content['root_path'])[-1] == os.path.split(self.project_dir)[-1]
            # ]

            model_names = [model['metadata']['name'] for model in models_selected]
            qty_models = len(models_selected)
            logging.info(f"...Processing {qty_models} models...")

            if models_selected:
                logging.info(f"{model_names}")

            return models_selected

    def build_objs(self):
        """Writes on yaml file the schema test."""
        for model in self.models:
            try:
                model_obj = self._build_model_obj(model)
                self.model_objs.append(model_obj)
            except Exception as e:
                logging.info(f"Error building model object with error: '{e}'")

    def _build_model_obj(self, model):
        result = OrderedDict({
            "version": self.version,
            "models": [
                OrderedDict({
                    "name": model['metadata']['name'],
                    "description": model['metadata']['comment'],
                    'original_file_path': '',  # TODO: needs a helper
                    "dbt_utils.recency":
                        OrderedDict({
                            'datepart': 'day',
                            'field': 'etl_updated_timestamp',
                            'interval': 1,
                            'tags': 'timeliness',
                            'severity': 'warn',
                        }),
                    "dbt_expectations.expect_column_distinct_count_to_equal_other_table":
                        OrderedDict({
                            'column_name': 'job_source_id',
                            'row_condition': "COLUMN_MODEL <> '-1'",
                            'compare_model': "ref('COLUMN_MODEL')",
                            'compare_column_name': 'COLUMN_MODEL',
                            'compare_row_condition': True,
                            'tags': 'completeness',
                            'severity': 'warn',
                        }),
                    'meta': OrderedDict({
                        'slo': OrderedDict({
                            'included': True,
                            'delivery_time_ct': None,
                            'offset': None,
                            'is_static': None,
                            'contact': None,
                            'tag': None
                        }),
                        'ownership': OrderedDict({
                            'team_id': None,
                            'ddo_delegate': None,
                            'ddo_delegate_ldap': None,
                            'subject_matter_expert': None,
                            'subject_matter_expert_ldap': None,
                            'approver': None,
                            'approver_ldap': None,
                            'approver_role': None
                        }),
                        'detailed_description': OrderedDict({
                            'dataset_granularity': None,
                            'classification': None,
                            'classification_rationale': None,
                            'sensitive': None,
                            'is_degraded': None,
                            'comments': None
                        }),
                        'build_requirements': OrderedDict({
                            'link_to_builder_code': None,
                            'link_to_orchestration_tool': None,
                            'link_to_design_document': None
                        }),
                        'origination': OrderedDict({
                            'refresh_cadence': None,
                            'refresh_schedule': None
                        }),
                        'filters_and_limitations': OrderedDict({
                            'standard_filters_at_consumption': None,
                            'general_limitations': None,
                            'field_discontinuities_irregularities': None,
                            'missing_data': None
                        }),
                        'data_access': OrderedDict({
                            'access_controls': None,
                            'access_controls_review': None
                        }),
                        'data_quality': OrderedDict({
                            'health_check': None
                        })
                    })
                })
            ]
        })

        # this should work with catalog -seems to me
        cols = self._build_columns_obj(model)
        if cols:
            result["models"][0]["columns"] = cols
        logging.debug(f" Model {model['name']} processed! ----")
        return result

    def _build_columns_obj(self, model):
        return OrderedList(
            [
                OrderedDict({
                    'name': column['name'],
                    'description': column['comment'],  # TODO: which is the field
                    'tests': OrderedList(
                        [
                            OrderedDict({'not_null':
                                OrderedDict({
                                    'tags': 'validity',
                                    'severity': 'warn'
                                })
                            }),
                            OrderedDict({'unique':
                                OrderedDict({
                                    'tags': 'validity',
                                    'severity': 'warn'
                                })
                            }),
                        ]
                    ),
                    'meta': OrderedDict({
                        'type': column['type'],
                        'privacy_classification': None,
                        'ldm_model': None,
                        'ldm_attribute': None,
                        'datasource': None,
                        'field': None,
                        'comments': ''
                    })
                })
                for column in model['columns'].values() if model['columns']
            ]
        )

    def process_models(self):
        """Iterate over the models to be written on the output yaml."""
        for model_obj in self.model_objs:
            is_processed = False
            try:
                is_processed = self.write_yaml(model_obj)
            except Exception as error:
                logging.error(
                    f"Error processing {model_obj['models'][0]['name']} with error '{error}'")
            if is_processed:
                logging.info(
                    f"Successfully processed {model_obj['models'][0]['name']}")
            else:
                logging.warning(
                    f"Skipped model {model_obj['models'][0]['name']}. Add -u flag to update all the existing models.")

    def write_yaml(self, obj):
        """Writes on yaml file the schema test."""
        yml = yaml.YAML()
        yml.indent(mapping=2, sequence=4, offset=2)
        model_name = obj['models'][0].get('name')
        original_path = obj['models'][0].get('original_file_path')
        obj['models'][0].pop('original_file_path')
        if model_name:
            file_name = f"{self.project_dir}/{original_path}"[:-3]
            file_name = "path\\to\\app\\"  # TODO: absolute PATH, fix next commit to relative
            file_name = file_name + 'schema_test.yml'  # TODO: output name hardcoded, confirm if need something dynamic
            if exists(file_name) and not self.update:
                return False
            else:
                with open(file_name, 'w+') as f:
                    obj['version'] = 2
                    yml.dump(obj, f)
                return True


@click.command(help="This script generates schema test yml files for models within a dbt project")
@click.option("--project-dir", "-pd", default='/app/',
              help=("The path to the dbt project for which to generate schema tests. " +
                    "The default is dbt_transform."))
@click.option("--model", "-m", default=None,
              help=("The name of the model to generate schema tests for.  If not passed " +
                    "schema tests will be generated for all models within the project."))
@click.option("--update", "-u", is_flag=True,
              help="A flag that determines whether or not the yml files will be updated.")
def main(project_dir, model, update):
    builder = SchemaBuilder(project_dir=project_dir,
                            model_selected=model, update=update)
    builder.start()
    logging.info("---- Schema Builder Utility Finished! ----\n")


if __name__ == "__main__":
    main()
