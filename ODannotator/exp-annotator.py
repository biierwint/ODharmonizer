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
This script is part of the ODharmonizer-v1.0 > ODannotator
The purpose of this script is to annotate expression data (gene/transcript/protein expression).

Example usage: exp-annotator.py --in GEx.csv  --out annotated-gex.csv --api_url "http://localhost:8000/api/odmapper/gene/GENCODE/"

'''


import psycopg2
import pandas as pd
import argparse
import sys
import os
import traceback
from datetime import datetime
from typing import Union

from annotator_modules.omop_loader import *

def main():
    try:
        parser = ODArgumentParser(description="Create measurement table from expression data")
        parser.add_argument("--in", dest="infile", required=True, help="input file containing expression data with gene in rows and samples in columns (.csv)", type=str)
        parser.add_argument("--out", dest="outfile", required=True, help="annotated expression data", type=str)
        parser.add_argument("--api", dest="api", required=True, help="ODmapper API URL", type=str)

        # Show help if no arguments are provided
        if len(sys.argv) == 1:
            parser.print_help()
            sys.exit(1)

        args = parser.parse_args()

        ### ETL
        loader = OMOP_Annotator (api_url=args.api)

        # load Expression data (infile)
        #gex_df = pd.read_csv(args.infile, on_bad_lines='warn')
        # Check if file is gzipped
        if args.infile.endswith(".gz"):
            gex_df = pd.read_csv(args.infile, compression='gzip', on_bad_lines='warn')
        else:
            gex_df = pd.read_csv(args.infile, on_bad_lines='warn')

        # map concept_id
        gene_col = gex_df.columns[0]
        gene_list = gex_df[gene_col].tolist()
        gene_map = loader.map_gene_concept_ids(gene_list)

        # Convert dict to DataFrame
        gene_df = pd.DataFrame(gene_map.items(), columns=[gene_col, "concept_id"])

        # Merging final GEx data
        final_df = pd.merge(gene_df, gex_df, on=gene_col, how='left')

        final_df.to_csv(args.outfile, index=False)

        print ('Finished Expression Annotator')

    except Exception as e:
        print (f'An error occurred: {e}')
        traceback.print_exc()

if __name__ == '__main__':
    main()
