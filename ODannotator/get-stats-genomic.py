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
The purpose of this script is to get the mapping statistics of annotated VCF data.

Example usage: ./get-stats-genomic.py annotated-vcf.csv.gz

'''

import sys
import gzip

def parse_vcf_line(line):
    fields = line.strip().split('\t')
    info_field = fields[7]
    info_dict = dict(item.split('=', 1) for item in info_field.split(';') if '=' in item)
    concept_ids = info_dict.get('OMOP_Concept_IDs', '')
    return concept_ids.split(',') if concept_ids else ['-', '-']

def main(vcf_path):
    open_func = gzip.open if vcf_path.endswith('.gz') else open
    total_variants = 0
    fully_annotated = 0
    unannotated = 0
    annotated_ref = 0
    annotated_alt = 0

    with open_func(vcf_path, 'rt') as f:
        for line in f:
            if line.startswith('#'):
                continue
            total_variants += 1
            concept_ids = parse_vcf_line(line)
            ref_id = concept_ids[0] if len(concept_ids) > 0 else '-'
            alt_id = concept_ids[1] if len(concept_ids) > 1 else '-'

            if ref_id != '-':
                annotated_ref += 1
            if alt_id != '-':
                annotated_alt += 1
            if ref_id != '-' and alt_id != '-':
                fully_annotated += 1

    print(f"Total variants: {total_variants}")
    print(f"Fully annotated variants (REF & ALT): {fully_annotated}")
    print(f"Variants with annotated REF: {annotated_ref}")
    print(f"Variants with annotated ALT: {annotated_alt}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python get-stats-genomic.py <path_to_annotated_omop.vcf[.gz]>")
        sys.exit(1)
    main(sys.argv[1])

