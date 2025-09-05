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
It takes in the person_source_value file which contains the person_source_value and optionally its associated fields for specimen table.

Additionally, user can override two fields in the specimen table:
(1) specimen_concept_id
If None is provided, the default value is 4047495 (peripheral blood measurement).
(2) specimen_type_concept_id
If None is provided, the default value is 32856 (Lab)

Example usage: ./create-specimen.py --in person_source.csv  --out specimen.csv

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
        parser = ODArgumentParser(description="Create specimen table")
        parser.add_argument("--in", dest="infile", required=True, help="input file containing person_source_value (.csv)", type=str)
        parser.add_argument("--out", dest="outfile", required=True, help="output file OMOP CDM specimen table", type=str)
        parser.add_argument("--specimen", dest="specimen_concept_id", help="concept id for specimen", nargs="?", type=int, default=None)
        parser.add_argument("--sctid", dest="specimen_type_concept_id", help="concept id specimen_type_concept_id", nargs="?", type=int, default=None)
        parser.add_argument("--prefix", dest="prefix", help="prefix to generate specimen_id [optional]", nargs="?", type=str, default=None)

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

        if args.specimen_concept_id is not None and 'specimen_concept_id' in person_df.columns:
            warnings.warn("specimen_concept_id exists in both command-line parameter and input file. The parameter value will override the column.")

        if args.specimen_type_concept_id is not None and 'specimen_type_concept_id' in person_df.columns:
            warnings.warn("specimen_type_concept_id exists in both command-line parameter and input file. The parameter value will override the column.")

        specimen = loader.generate_specimen_dataframe (
                           samples_mapping,
                           default_specimen_concept_id=args.specimen_concept_id,
                           default_specimen_type_concept_id=args.specimen_type_concept_id,
                           prefix=args.prefix
                       )

        specimen.to_csv(args.outfile, index=False)

        # if want to insert into observation table
        if args.dbwrite:
            print ('Insert into specimen table')
            loader.insert_table (specimen, "specimen")

        loader.close()

        print ('Finished create specimen table...')

    except Exception as e:
        print (f'An error occurred: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    main()
