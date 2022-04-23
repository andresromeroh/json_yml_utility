# json_yml_utility
Read from JSON file, generate schema_test.yml from SQL models based on their metada-

1) Make the script work: *
---- load/read models & manifest directories correctly
---- create schema test for all model, if no args passed on CLI
---- create schema test for one model, if by args passed 

* the code works, may just change some directory paths-

2) change the exact same output yml file, to be generate with catalog.json instead of manifest.json they may differ on attr positions and dict structures

3) Once all previous have been master; look forward to generate this type of expected final output for the version 2.0 of the schema_test.ymls made out from catalog.json

4) Unit test the Class methods from utility script

5) ReadMe.md the utility for the given new enhancements/usability

6) Be proud of being the owner of this package, have fun


--------------------------------
From client's description:
Drive the creation of .yml files using the catalog.json file instead of the manifest.json file (includes columns names and data types).

Ensure for each column the name and data type are populated in addition to defaults with description, tests (not null), etc.
That the user will then manually update later on.

line 76 is the obj --> model_obj = self._build_model_obj(model)  # THIS IS stuff.json
https://github.com/Python-Revolution/json_yml_utility/blob/6026462f3dde1313256561990f797ac8e146eac0/app/create_schema_test.py#L76
