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
The purpose of this script is to get the mapping statistics of annotated expression data (gene/transcript/protein expression).

Example usage: ./get-stats-expression.py annotated-gex.csv

'''

import sys
import pandas as pd

def main(csv_path):
    df = pd.read_csv(csv_path, on_bad_lines='warn')

    total_genes = df.shape[0]
    mapped = df['concept_id'].notna().sum()
    unmapped = df['concept_id'].isna().sum()
    total_subjects = df.shape[1] - 2  # exclude ensg_id and concept_id

    print(f"Total gene IDs (rows): {total_genes}")
    print(f"Mapped concept_ids: {mapped}")
    print(f"Unmapped concept_ids (None): {unmapped}")
    print(f"Total subjects (samples): {total_subjects}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python get-stats-expression.py <path_to_expression.csv>")
        sys.exit(1)
    main (sys.argv[1])

