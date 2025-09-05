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
The purpose of this script is to create measurement table and fact_relationship table from the Annotated Genomic data (through ODannotator)
It takes in the person_source_value file which contains the person_source_value and optionally its associated fields for measurement table.

Additionally, user can override two fields in the measurement table:
(1) meas_event_field_concept_id
If None is provided, the default value is 1147165 (observation.observation_id).
(2) measurement_type_concept_id
If None is provided, the default value is 32856 (Lab).

Example usage: ./vcf2measurement.py --in omop-dummy-genomic-data.vcf.gz  --obs observation-genomic.csv --specimen specimen-genomic.csv --out measurement-genomic.csv --outfact fact_relationship-genomic.csv
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
        parser = ODArgumentParser(description="Create measurement table from gene expression data")
        parser.add_argument("--person", dest="personfile", required=True, help="input file containing person_source_value mapping (.csv)", type=str)
        parser.add_argument("--in", dest="infile", required=True, help="input file containing OMOP-annotated vcf file", type=str)
        parser.add_argument("--obs", dest="obsfile", required=True, help="observation table file containing observation_id,person_id,observation_date (.csv)", type=str)
        parser.add_argument("--specimen", dest="specimenfile", required=True, help="specimen table file containing specimen_id and person_id (.csv)", type=str)
        parser.add_argument("--out", dest="outfile", required=True, help="output file OMOP CDM measurement table", type=str)
        parser.add_argument("--outfact", dest="outfact", required=True, help="output file OMOP CDM fact_relationship table", type=str)
        parser.add_argument("--mefid", dest="meas_event_field_concept_id", help="concept id for meas_event_field_concept_id", nargs="?", type=int, default=None)
        parser.add_argument("--mtid", dest="measurement_type_concept_id", help="concept id for measurement_type_concept_id", nargs="?", type=int, default=None)
        parser.add_argument("--start", dest="start_index", help="start index for measurement_id", nargs="?", type=int, default=None)
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

        # load person table (personfile)
        person = pd.read_csv(args.personfile, on_bad_lines='warn')
        person_df = loader.get_person_id (person)

        if args.meas_event_field_concept_id is not None:
            if 'meas_event_field_concept_id' in person_df.columns:
                warnings.warn("meas_event_field_concept_id exists in both command-line parameter and input file. The parameter value will override the column")
            person_df['meas_event_field_concept_id'] = args.meas_event_field_concept_id
        elif 'meas_event_field_concept_id' not in person_df.columns:
            person_df['meas_event_field_concept_id'] = 1147165  # default value

        if args.measurement_type_concept_id is not None:
            if 'measurement_type_concept_id' in person_df.columns:
                warnings.warn("measurement_type_concept_id exists in both command-line parameter and input file. The parameter value will override the column")
            person_df['measurement_type_concept_id'] = args.measurement_type_concept_id
        elif 'measurement_type_concept_id' not in person_df.columns:
            person_df['measurement_type_concept_id'] = 32856    # default value

        person_map = person_df.set_index('person_source_value').to_dict(orient='index')

        # load  data (infile)
        vcf_df = loader.convert_vcf_to_row_col_format(vcf_path = args.infile)

        '''
        # map the person_source_value to person_id
        person_columns = vcf_df.columns[2:]
        person_df = pd.DataFrame(person_columns, columns=['person_source_value'])
        person_df = loader.get_sample_id (person_df)
        person_df['visit_occurrence_id'] = None	# if need this in the future, please put inside obs_map
        person_df['visit_detail_id'] = None # if need this in the future, please put inside obs_map
        #person_df['unit_concept_id'] = args.unit_concept_id
        #person_df['unit_source_value'] = args.unit_source_value
        person_df['meas_event_field_concept_id'] = args.meas_event_field_concept_id
        person_df['measurement_type_concept_id'] = args.measurement_type_concept_id	# Lab
        person_map = person_df.set_index('person_source_value').to_dict(orient='index')
        '''

        # create dictionary
        id_map = vcf_df.set_index("ID")["OMOP_Concept_ID"].to_dict()

        # load observation table (obsfile)
        observation_df = pd.read_csv(args.obsfile, on_bad_lines='warn')
        obs_map = loader.get_observation_to_measurement_dict (observation_df)

        # read specimen file
        specimen_df = pd.read_csv(args.specimenfile, on_bad_lines='warn')

        # preparing measurement cdm table
        vcf_df = vcf_df.drop(columns=["OMOP_Concept_ID"])

        (measurement_df, fact_relationship) = loader.generate_genomic_measurement_dataframe (vcf_df, id_map, obs_map, person_map, specimen_df, start_index=args.start_index)

        measurement_df.to_csv(args.outfile, index=False)
        fact_relationship.to_csv(args.outfact, index=False)
        
        # if want to insert into measurement table
        if args.dbwrite:
            print ('Insert into measurement table')
            loader.insert_table(measurement_df, "measurement")
            loader.insert_table(fact_relationship, "fact_relationship")

        loader.close()

        print ('Finished create genomic measurement table and fact_relationship table...')

    except Exception as e:
        print (f'An error occurred: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    main()
