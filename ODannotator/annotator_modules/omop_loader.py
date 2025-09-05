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
This script is part of the ODharmonizer-v1.0 > ODannotator.
It contains the functions to map omics_id to OMOP concept_ids

'''
import psycopg2
import pandas as pd
import argparse
import sys
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class OMOP_Annotator:
    def __init__(self, api_url=None):
        self.api_url = api_url

    # map gene concept_id
    def get_concept_id (self, gene: str) -> int | None:
        try:
            response = requests.get(f"{self.api_url}{gene}")
            if response.status_code == 200:
                return response.json().get('concept_id')
        except Exception as e:
            print(f"API error for {gene}: {repr(e)}")
        return None

    '''
    def map_gene_concept_ids (self, genes: list[str]) -> dict:
        gene_map = {}
        for gene in tqdm(genes, desc="Fetching genes' concept_ids"):
            gene_map[gene] = self.get_concept_id(gene)
        return gene_map
    '''

    def map_gene_concept_ids(self, genes: list[str]) -> dict:
        gene_map = {}

        max_workers = 8  # Adjust based on your system/API capacity

        start_time = time.time()
        print(f"Annotating expression data: {len(genes)} records...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.get_concept_id, gene): gene for gene in genes}

            for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching genes' concept_ids"):
                gene = futures[future]
                try:
                    concept_id = future.result()
                    gene_map[gene] = concept_id
                except Exception as e:
                    print(f"Error fetching concept_id for {gene}: {repr(e)}")
                    gene_map[gene] = None

        elapsed = time.time() - start_time
        rate = len(genes) / elapsed if elapsed > 0 else 0
        print(f"Finished annotating {len(genes)} records in {elapsed:.2f} seconds ({rate:.2f} records/s)")

        return gene_map

class ODArgumentParser (argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"Error: {message}\n\n")
        self.print_help()
        sys.exit(2)



