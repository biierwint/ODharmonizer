#!/usr/bin/env python3

# Copyright (C) 2025 A*STAR

# ODmapper (Omics Data Mapping and Harmonizer) is an effort by the
# Data Management Platform in the Bioinformatics Institute (BII),
# Agency of Science, Technology and Research (A*STAR), Singapore.

# This file is part of ODannotator

# ODannotator is an open-source tool.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 you can redistribute it and/or modify
# it under the terms of the https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# You should have received a copy of the Apache License, Version 2.0
# along with this program.  If not, see <https://www.apache.org/licenses/LICENSE-2.0>

'''
This script is part of the ODharmonizer-v1.0 > ODconverter
The purpose of this script is to create an observation table.
It takes in the person_source_value file which contains the person_source_value and optionally its associated fields for observation table.

Additionally, user can override four fields in the observation table:
(1) observation_concept_id
If None is provided, the default value is 21495062 (Variant analysis method [Type]).
(2) observation_type_concept_id
If None is provided, the default value is 32856 (Lab).
(3) value_as_concept_id
If None is provided, the default value is 42531068 (Gene Expression Array). For SNP Array, the value_as_concept_id = 42530745.
For Sequencing, the value_as_concept_id = 42531016
(4) value_source_value
If None is provided, the default value is "Gene Expression Array"
(5) oefid (obs_event_field_concept_id)
If None is provided, the default value is 1147049 (specimen.specimen_id)
Example usage: ./create-observation.py --in person_source.csv  --out observation.csv

'''
import psycopg2
import pandas as pd
import argparse
import sys
import os
import traceback
from datetime import datetime
from typing import Union
from dotenv import load_dotenv

from converter_modules.omop_loader import *

def main():
    try:
        parser = ODArgumentParser(description="Create observation table for OMICs data")
        parser.add_argument("--in", dest="infile", required=True, help="input file containing person_source_value [person_id, observation_concept_id, observation_date, etc] (.csv file)", type=str)
        parser.add_argument("--out", dest="outfile", required=True, help="output file OMOP CDM observation_period table", type=str)
        parser.add_argument("--cid", dest="obs_conceptid", help="concept id to fill for observation_concept_id", nargs="?", type=int, default=None)
        parser.add_argument("--otid", dest="obs_type_conceptid", help="concept id to fill for observation_type_concept_id", nargs="?", type=int, default=None)
        parser.add_argument("--vid", dest="value_conceptid", help="concept id to fill value_as_concept_id", nargs="?", type=int, default=None)
        parser.add_argument("--vsource", dest="value_source_value", help="value_source_value of value_as_concept_id", nargs="?", type=str, default=None)
        parser.add_argument("--specimen", dest="specimenfile", required=True, help="specimen table file containing specimen_id and person_id (.csv)", type=str)
        parser.add_argument("--oefid", dest="obs_event_field_concept_id", help="concept id for obs_event_field_concept_id",nargs="?", type=int, default=None)
        parser.add_argument("--start", dest="start_index", help="start index for observation_id", nargs="?", type=int, default=None)
        parser.add_argument("--dbwrite", action='store_true', help="write to database if this flag is present")

        # Show help if no arguments are provided
        if len(sys.argv) == 1:
            parser.print_help()
            sys.exit(1)

        args = parser.parse_args()

        ### Database configuration
        load_dotenv()	# Load from .env file

        db_config = {
            "dbname": os.getenv("DB_NAME"),
            "schema": os.getenv("DB_SCHEMA"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", 5432))  # default to 5432 if not set
        }

        ### ETL
        loader = OMOP_ODmapper(db_config)
        person_df = pd.read_csv(args.infile, on_bad_lines='warn')
        samples_mapping = loader.get_person_id (person_df)

        if args.obs_conceptid is not None and 'observation_concept_id' in person_df.columns:
            warnings.warn("observation_concept_id exists in both command-line parameter and person file. The parameter value will override the column.")

        if args.obs_type_conceptid is not None and 'observation_type_concept_id' in person_df.columns:
            warnings.warn("observation_type_concept_id exists in both command-line parameter and person file. The parameter value will override the column.")

        if args.value_conceptid is not None and 'value_as_concept_id' in person_df.columns:
            warnings.warn("value_as_concept_id exists in both command-line parameter and person file. The parameter value will override the column.")

        if args.value_source_value is not None and 'value_source_value' in person_df.columns:
            warnings.warn("value_source_value exists in both command-line parameter and person file. The parameter value will override the column.")

        if args.obs_event_field_concept_id is not None and 'obs_event_field_concept_id' in person_df.columns:
            warnings.warn("obs_event_field_concept_id exists in both command-line parameter and person file. The parameter value will override the column.")

        # load specimen table (specimenfile)
        specimen_df = pd.read_csv(args.specimenfile, on_bad_lines='warn')
        specimen_map = loader.get_specimen_to_person_dict (specimen_df)

        # Merged samples_mapping and specimen_map
        samples_mapping['observation_event_id'] = samples_mapping['person_id'].map(specimen_map)

        observations = loader.generate_observation_dataframe (
                           samples_mapping,
                           default_observation_concept_id=args.obs_conceptid,
                           default_observation_type_concept_id = args.obs_type_conceptid,
                           default_value_as_concept_id=args.value_conceptid,
                           default_value_source_value=args.value_source_value,
                           default_obs_event_field_concept_id=args.obs_event_field_concept_id,
                           start_index=args.start_index
                       )

        observations.to_csv(args.outfile, index=False)

        # if want to insert into observation table
        if args.dbwrite:
            print ('Insert into observation table')
            loader.insert_table (observations, "observation")

        loader.close()

        print ('Finished create observation table...')

    except Exception as e:
        print (f'An error occurred: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    main()
