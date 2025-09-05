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
The purpose of this script is to insert table to OMOP CDM

Example usage: ./insert_table.py --in cdm_table.csv --table measurement
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
        parser = ODArgumentParser(description="Insert data into OMOP CDM tables")
        parser.add_argument("--in", dest="infile", required=True, help="input file containing entries following OMOP CDM table(.csv)", type=str)
        parser.add_argument("--table", dest="table", required=True, help="OMOP CDM table name", type=str)

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

        # load data (infile)
        df = pd.read_csv(args.infile, on_bad_lines='warn')
        df = df.where(pd.notnull(df), None)	# convert NaN to None
        table_name = args.table

        print (f'Insert into {table_name} table')
        loader.insert_table (df, table_name)
        loader.close()

        print ('Finished inserting table...')

    except Exception as e:
        print (f'An error occurred: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    main()
