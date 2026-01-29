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
This script is part of the ODharmonizer-v1.0 > ODconverter.
It contains the functions to create observation, specimen table. Also to convert omics data to measurement and fact_relationship table.
'''
import psycopg2
import pandas as pd
import numpy as np
import argparse
import sys
import requests
import gzip
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

class OMOP_ODmapper:
    def __init__(self, db_config, api_url=None):
        self.db_config = db_config
        self.db_schema = db_config['schema']
        self.db_uri = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        self.engine = create_engine(self.db_uri)
        self.connection = self.engine.connect()
        self.api_url = api_url

    def get_person_id (self, df) -> pd.DataFrame:
        """
        Get person_id from a df['person_id'] or from database (according to df['person_source_value'])

        Parameters:
        - df: a dataframe containing person_source_value [,person_id, etc]

        """
        # assume the source_csv contains the person_id and therefore no need to lookup for person_id
        lookup_person_id = False
        # lookup_person_id:
        #    - True: df contains only 'person_source_value'; get person_id from DB.
        #    - False: df contains both 'person_id' and 'person_source_value'.

        if 'person_id' not in df.columns:
            lookup_person_id = True

        if lookup_person_id:
            if 'person_source_value' not in df.columns:
                raise ValueError("CSV must contain 'person_source_value' for lookup.")
            source_values = df['person_source_value'].tolist()
            placeholders = ','.join(['%s'] * len(source_values))
            query = f"""
                SELECT person_id, person_source_value
                FROM {self.db_schema}.person
                WHERE person_source_value IN ({placeholders})
            """
            #db_df = pd.read_sql(query, self.conn, params=source_values)
            #db_df = pd.read_sql(query, con=self.engine, params=source_values)
            db_df = pd.read_sql(query, con=self.engine, params=tuple(source_values))

            if db_df.empty:
                raise ValueError("No matching person_source_value found in the person table.")

            # Merge original df with db_df to align person_id with the original row order
            df = df.merge(db_df, on="person_source_value", how="left")
            if df['person_id'].isnull().any():
                missing = df[df['person_id'].isnull()]['person_source_value'].tolist()
                raise ValueError(f"Missing person_id for: {missing}")
        else:
            if not {'person_id', 'person_source_value'}.issubset(df.columns):
                raise ValueError("CSV must contain both 'person_id' and 'person_source_value'.")

        return df

    def generate_observation_dataframe(
        self,
        df: pd.DataFrame,
        default_observation_concept_id: int = None,
        default_observation_type_concept_id: int = None,
        default_value_as_concept_id: int = None,
        default_value_source_value: str = None,
        default_obs_event_field_concept_id=None,
        start_index = None,
    ) -> pd.DataFrame:
        """
        Generate a DataFrame compatible with OMOP CDM `observation` table from the input.
        """
        today = datetime.today().strftime('%Y-%m-%d')

        records = []

        if "observation_id" not in df.columns:
            if start_index is not None:
                df['observation_id'] = range(start_index, start_index + len(df))
            else:
                df['observation_id'] = range(1, len(df)+1)

        for _, row in df.iterrows():
            obs_date = (
                row['observation_date']
                if 'observation_date' in df.columns and pd.notna(row['observation_date'])
                else today
            )
            obs_concept_id = (
                default_observation_concept_id
                if default_observation_concept_id is not None
                else row['observation_concept_id']
                if 'observation_concept_id' in df.columns and pd.notna(row['observation_concept_id'])
                else 21495062
            )
            obs_type_concept_id = (
                default_observation_type_concept_id
                if default_observation_type_concept_id is not None
                else row['observation_type_concept_id']
                if 'observation_type_concept_id' in df.columns and pd.notna(row['observation_type_concept_id'])
                else 32856
            )
            val_concept_id = (
                default_value_as_concept_id
                if default_value_as_concept_id is not None
                else row['value_as_concept_id']
                if 'value_as_concept_id' in df.columns and pd.notna(row['value_as_concept_id'])
                else 42531068
            )
            value_source_value = (
                default_value_source_value
                if default_value_source_value is not None
                else row['value_source_value']
                if 'value_source_value' in df.columns and pd.notna(row['value_source_value'])
                else 'Gene Expression Array'
            )
            obs_event_field_concept_id = (
                default_obs_event_field_concept_id
                if default_obs_event_field_concept_id is not None
                else row['obs_event_field_concept_id']
                if 'obs_event_field_concept_id' in df.columns and pd.notna(row['obs_event_field_concept_id'])
                else 1147049	# specimen.specimen_id
            )

            if obs_concept_id is None:
                raise ValueError("observation_concept_id is required (in column or as default)")

            record = {
                "observation_id": row.get('observation_id'),
                "person_id": row.get('person_id'),
                "observation_concept_id": obs_concept_id,
                "observation_date": obs_date,
                "observation_datetime": obs_date,
                "observation_type_concept_id": obs_type_concept_id,	# observation_type_concept_id: Lab
                "value_as_number": row.get('value_as_number') if 'value_as_number' in df.columns else None,
                "value_as_string": row.get('value_as_string') if 'value_as_string' in df.columns else None,
                "value_as_concept_id": val_concept_id,
                "qualifier_concept_id": row.get('qualifier_concept_id') if 'qualifier_concept_id' in df.columns else None,
                "unit_concept_id": row.get('unit_concept_id') if 'unit_concept_id' in df.columns else None,
                "provider_id": row.get('provider_id') if 'provider_id' in df.columns else None,
                "visit_occurrence_id": row.get('visit_occurrence_id') if 'visit_occurrence_id' in df.columns else None,
                "visit_detail_id": row.get('visit_detail_id') if 'visit_detail_d' in df.columns else None,
                "observation_source_value": value_source_value,
                "observation_source_concept_id": row.get('observation_source_concept_id') if 'observation_source_concept_id' in df.columns else None,
                "unit_source_value": row.get('unit_source_value') if 'unit_source_value' in df.columns else None,
                "qualifier_source_value": row.get('qualifier_source_value') if 'qualifier_source_value' in df.columns else None,
                "value_source_value": value_source_value,
                "observation_event_id": row.get('observation_event_id') if 'observation_event_id' in df.columns else None,
                #"obs_event_field_concept_id": row.get('obs_event_field_concept_id') if 'obs_event_field_concept_id' in df.columns else None
                "obs_event_field_concept_id": obs_event_field_concept_id
            }

            records.append(record)

        return pd.DataFrame(records)

    def generate_specimen_dataframe(
        self,
        df: pd.DataFrame,
        default_specimen_concept_id: int = None,
        default_specimen_type_concept_id: int = None,
        start_index = None,
    ) -> pd.DataFrame:
        """
        Generate a DataFrame compatible with OMOP CDM `specimen` table from the input.
        """
        today = datetime.today().strftime('%Y-%m-%d')

        records = []

        if "specimen_id" not in df.columns:
            if start_index is not None:
                df['specimen_id'] = range(start_index, start_index + len(df))
            else:
                df['specimen_id'] = range(1, len(df)+1)

        for _, row in df.iterrows():
            specimen_date = (
                row['specimen_date']
                if 'specimen_date' in df.columns and pd.notna(row['specimen_date'])
                else today
            )
            specimen_concept_id = (
                default_specimen_concept_id
                if default_specimen_concept_id is not None
                else row['specimen_concept_id']
                if 'specimen_concept_id' in df.columns and pd.notna(row['specimen_concept_id'])
                else 4047495
            )
            specimen_type_concept_id = (
                default_specimen_type_concept_id
                if default_specimen_type_concept_id is not None
                else row['specimen_type_concept_id']
                if 'specimen_type_concept_id' in df.columns and pd.notna(row['specimen_type_concept_id'])
                else 32856
            )

            record = {
                "specimen_id": row.get('specimen_id'),
                "person_id": row.get('person_id'),
                "specimen_concept_id": specimen_concept_id,
                "specimen_date": specimen_date,
                "specimen_datetime": specimen_date,
                "specimen_type_concept_id": specimen_type_concept_id,
                "quantity": row.get('quantity') if 'quantity' in df.columns else None,
                "unit_concept_id": row.get('unit_concept_id') if 'unit_concept_id' in df.columns else None,
                "anatomic_site_concept_id": row.get('anatomic_site_concept_id') if 'anatomic_site_concept_id' in df.columns else None,
                "disease_status_concept_id": row.get('disease_status_concept_id') if 'disease_status_concept_id' in df.columns else None,
                "specimen_source_id": row.get('specimen_source_id') if 'specimen_source_id' in df.columns else None,
                "specimen_source_value": row.get('specimen_source_value') if 'specimen_source_value' in df.columns else None,
                "unit_source_value": row.get('unit_source_value') if 'unit_source_value' in df.columns else None,
                "anatomic_site_source_value": row.get('anatomic_site_source_value') if 'anatomic_site_source_value' in df.columns else None,
                "disease_status_source_value": row.get('disease_status_source_value') if 'disease_status_source_value' in df.columns else None,
            }

            records.append(record)

        return pd.DataFrame(records)

    def insert_table (self, cdm_df: pd.DataFrame, table_name: str):
        if cdm_df.empty:
            print("Warning: No measurement to insert.")

        try:
            cdm_df = cdm_df.replace({np.nan: None})
            cdm_df = cdm_df.replace(["No Data", "NA", "", "null"], None)
            records = cdm_df.to_dict(orient="records")
            columns = cdm_df.columns.tolist()
            col_names = ", ".join(columns)
            placeholders = ", ".join([f":{col}" for col in columns])

            insert_stmt = text(f"""
                INSERT INTO {self.db_schema}.{table_name} ({col_names})
                VALUES ({placeholders})
            """)
            # Use session for transaction management
            with Session(self.engine) as session:
                with session.begin():
                    result = session.execute(insert_stmt, records)
                    num_inserted = result.rowcount

            print(f"Inserted {num_inserted} {table_name} within transaction.")

        except Exception as e:
            print(f"Fail: Failed to insert {table_name}: {e}")

    def get_observation_to_measurement_dict (self, df: pd.DataFrame) -> dict:
        """
        Load observation dataframe (df) and return a mapping from person_id to
        measurement_event_id (observation_id) and measurement_date.
        """
        obs_df = df.rename(columns={
            'observation_id': 'measurement_event_id',
            #'observation_date': 'measurement_date'	### initially, measurement_date is assumed to be observation_date. Now, have to define explicitly.
        })
        if not {'person_id', 'measurement_event_id', 'observation_date'}.issubset(obs_df.columns):
            raise ValueError("Observation file must contain 'observation_id', 'person_id', and 'observation_date' columns.")

        return obs_df.set_index('person_id')[['measurement_event_id', 'observation_date']].to_dict('index')

    def get_specimen_to_person_dict (self, df: pd.DataFrame) -> dict:
        """
        Load specimen dataframe (df) and return a mapping from person_id to
        observation_event_id (specimen_id).
        """
        specimen_df = df.rename(columns={
            'specimen_id': 'observation_event_id',
        })
        if not {'person_id', 'observation_event_id'}.issubset(specimen_df.columns):
            raise ValueError("Specimen file must contain 'specimen_id', 'person_id' columns.")

        return specimen_df.set_index('person_id')['observation_event_id'].to_dict()

    # map gene concept_id
    def get_concept_id (self, gene: str) -> int | None:
        try:
            response = requests.get(f"{self.api_url}{gene}")
            if response.status_code == 200:
                return response.json().get('concept_id')
        except Exception as e:
            print(f"API error for {gene}: {repr(e)}")
        return None

    def map_gene_concept_ids (self, genes: list[str]) -> dict:
        gene_map = {}
        for gene in tqdm(genes, desc="Fetching genes' concept_ids"):
            gene_map[gene] = self.get_concept_id(gene)
        return gene_map

    def generate_gex_measurement_dataframe(
            self, expr_df, gene_map, obs_map, person_map, specimen_df,
            start_index=None,
            #high_concept_id=4328749,        # <-- High or Elevated, i.e. > 1 sd away
            #low_concept_id=4267416,      # <-- Low, i.e. < 1 sd away
            high_concept_id=4084765,        # <-- High or Elevated, i.e. > 1 sd away
            low_concept_id=4083207,      # <-- Low, i.e. < 1 sd away
            ref_concept_id=4084764   # <-- "within reference range"
        ) -> tuple[pd.DataFrame, pd.DataFrame]:
    
        gene_col = expr_df.columns[0]
    
        # -----------------------------------------------------------
        # 1. COMPUTE Z-SCORES ACROSS SAMPLES FOR EACH GENE
        # -----------------------------------------------------------
        # expr_df: genes x samples (raw or normalized counts)
        data_only = expr_df.set_index(gene_col)
    
        # z = (x - mean) / sd  (per gene)
        zscores = data_only.sub(data_only.mean(axis=1), axis=0) \
                           .div(data_only.std(axis=1).replace(0, np.nan), axis=0)
    
        # add z-scores back to the dataframe in long form later
        zscores = zscores.reset_index()
    
        # -----------------------------------------------------------
        # 2. Melt expression + z-score matrices
        # -----------------------------------------------------------
        long_expr = expr_df.melt(id_vars=gene_col, var_name='person_source_value', value_name='value_as_number')
        long_z = zscores.melt(id_vars=gene_col, var_name='person_source_value', value_name='zscore')
    
        # merge so each row has expression + z-score
        long_df = long_expr.merge(long_z, on=[gene_col, 'person_source_value'], how='left')
    
        # -----------------------------------------------------------
        # 3. Assign up/down/no-change concept IDs
        # -----------------------------------------------------------
        def classify(z):
            if pd.isna(z):
                return None
            if z > 1:
                return high_concept_id
            elif z < -1:
                return low_concept_id
            else:
                return ref_concept_id
    
        long_df['value_as_concept_id'] = long_df['zscore'].map(classify)
    
        # -----------------------------------------------------------
        # 4. ORIGINAL MAPPING LOGIC
        # -----------------------------------------------------------
        long_df['person_id'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('person_id'))
    
        long_df['measurement_concept_id'] = long_df[gene_col].map(gene_map)
        long_df['measurement_source_value'] = long_df[gene_col]
        long_df['measurement_source_concept_id'] = long_df['measurement_concept_id']
    
        long_df['measurement_event_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('measurement_event_id'))
    
        if "measurement_date" in person_map:
            long_df['measurement_date'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('measurement_date'))
        else:
            long_df['measurement_date'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('observation_date'))
    
        long_df['measurement_datetime'] = long_df['measurement_date']
    
        # Map person-derived metadata
        for col in [
            'unit_concept_id', 'unit_source_value', 'measurement_type_concept_id',
            'meas_event_field_concept_id', 'measurement_time', 'operator_concept_id',
            'range_low', 'range_high', 'value_source_value'
        ]:
            long_df[col] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get(col))
    
        long_df['visit_occurrence_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('visit_occurrence_id'))
        long_df['visit_detail_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('visit_detail_id'))
        long_df['provider_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('provider_id'))
    
        # remove rows without gene concept
        long_df = long_df[long_df['measurement_concept_id'].notna()].copy()

        # Assign measurement_id
        if start_index is not None:
            long_df['measurement_id'] = range(start_index, start_index + len(long_df))
        else:
            long_df['measurement_id'] = None
    
        # -----------------------------------------------------------
        # 5. FACT RELATIONSHIP
        # -----------------------------------------------------------
        specimen_map = dict(zip(specimen_df['person_id'], specimen_df['specimen_id']))
        long_df['specimen_id'] = long_df['person_id'].map(specimen_map)
        long_df = long_df[long_df['specimen_id'].notna()].copy()
    
        fr_measure_to_spec = pd.DataFrame({
            'domain_concept_id_1': 1147330,
            'fact_id_1': long_df['measurement_id'],
            'domain_concept_id_2': 1147306,
            'fact_id_2': long_df['specimen_id'],
            'relationship_concept_id': 32668
        })
     
        fr_spec_to_measure = pd.DataFrame({
            'domain_concept_id_1': 1147306,
            'fact_id_1': long_df['specimen_id'],
            'domain_concept_id_2': 1147330,
            'fact_id_2': long_df['measurement_id'],
            'relationship_concept_id': 32669
        })
     
        fact_relationship_df = pd.concat([fr_measure_to_spec, fr_spec_to_measure], ignore_index=True)

        # -----------------------------------------------------------
        # 6. FINAL COLUMNS
        # -----------------------------------------------------------
        omop_cols = [
            'measurement_id', 'person_id', 'measurement_concept_id', 'measurement_date',
            'measurement_datetime', 'measurement_time', 'measurement_type_concept_id',
            'operator_concept_id', 'value_as_number', 'value_as_concept_id',
            'unit_concept_id', 'range_low', 'range_high', 'provider_id',
            'visit_occurrence_id', 'visit_detail_id', 'measurement_source_value',
            'measurement_source_concept_id', 'unit_source_value', 'value_source_value',
            'measurement_event_id', 'meas_event_field_concept_id'
        ]

        return long_df[omop_cols], fact_relationship_df

    def parse_genotype(self, gt_raw):
        """Normalize genotype: handle missing, split alleles"""
        if gt_raw.strip() in ['-1', '.', './.']:
            return None
        for sep in ['/', '|']:
            if sep in gt_raw:
                return gt_raw.split(sep)
        return None  # unexpected format

    def parse_info(self, info_field):
        """Extract OMOP Concept IDs"""
        info_parts = dict(item.split('=') for item in info_field.split(';') if '=' in item)
        omop_ids = info_parts.get('OMOP_Concept_IDs', '').split(',')
        return omop_ids

    def evaluate_genotype(self, alleles, current_allele):
        """Determine status for REF or ALT allele"""
        if alleles is None:
            return "Missing"

        alleles_str = "/".join(alleles)

        if current_allele in alleles:
            status = "positive"
        else:
            status = "negative"

        return f"{alleles_str}_{status}"

    def convert_vcf_to_row_col_format(self, vcf_path):
        output_rows = []

        #with open(vcf_path) as f:
        with gzip.open(vcf_path, "rt") as f:
            for line in f:
                if line.startswith('#CHROM'):
                    header = line.strip().split('\t')
                    sample_ids = header[9:]  # from 10th column onward
                    continue
                elif line.startswith('#'):
                    continue

                fields = line.strip().split('\t')
                chrom, pos, var_id, ref, alt, qual, filt, info, fmt = fields[:9]

                # Get the genotypes_raw
                #genotypes_raw = fields[9:]
                fmt_fields = fmt.split(':')
                gt_index = fmt_fields.index('GT') if 'GT' in fmt_fields else 0
                # Extract only the genotype (GT) from each sample field
                genotypes_raw = [g.split(':')[gt_index] for g in fields[9:]]

                alt_alleles = alt.split(',')
                all_alleles = [ref] + alt_alleles

                omop_ids = self.parse_info(info)
                ref_id = omop_ids[0] if len(omop_ids) >= 1 else "-"
                alt_ids = omop_ids[1:] if len(omop_ids) > 1 else []

                # Convert genotypes like 0/1 to real alleles like A/G
                converted_genotypes = []
                for gt in genotypes_raw:
                    parsed = self.parse_genotype(gt)
                    if parsed is None:
                        converted_genotypes.append(None)
                    else:
                        alleles = [all_alleles[int(i)] if i.isdigit() and int(i) < len(all_alleles) else None for i in parsed]
                        converted_genotypes.append(alleles)

                # Process REF
                if ref_id != '-':
                    row = [f"{var_id}_{ref}", ref_id]
                    row += [self.evaluate_genotype(alleles, ref) for alleles in converted_genotypes]
                    output_rows.append(row)

                # Process each ALT
                for i, (alt_allele, alt_id) in enumerate(zip(alt_alleles, alt_ids)):
                    if alt_id == '-':
                        continue
                    row = [f"{var_id}_{alt_allele}", alt_id]
                    row += [self.evaluate_genotype(alleles, alt_allele) for alleles in converted_genotypes]
                    output_rows.append(row)

        df = pd.DataFrame(output_rows, columns=["ID", "OMOP_Concept_ID"] + sample_ids)
        return df


    def generate_genomic_measurement_dataframe (self, genomic_df, id_map, obs_map, person_map, specimen_df, start_index = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        id_col = genomic_df.columns[0]

        # Melt using person_source_value columns directly (create a dataframe with fields: person_source_value and value_as_number)
        long_df = genomic_df.melt(id_vars=id_col, var_name='person_source_value', value_name='genomic_value')

        # Fill up avlue_source_value and value_as_concept_id
        # Split 'genomic_value' into allele and status
        long_df[['value_source_value', 'status']] = long_df['genomic_value'].str.split('_', expand=True)

        # Map status to OMOP concept IDs
        status_map = {
            'positive': 9191,
            'negative': 9189,
            'Missing': None
        }
        value_map = {
            'positive': 1,
            'negative': 0,
            'Missing': -1
        }
        long_df['value_as_concept_id'] = long_df['status'].map(status_map)
        long_df['value_as_number'] = long_df['status'].map(value_map)

        # For missing allele (no split result), set value_source_value = "./."
        long_df['value_source_value'] = long_df['value_source_value'].fillna('./.')

        # Map person_id (from person_map)
        long_df['person_id'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('person_id'))

        # Map measurement_concept_id from gene (from gene_map)
        long_df['measurement_concept_id'] = long_df[id_col].map(id_map)
        long_df['measurement_source_value'] = long_df[id_col]
        long_df['measurement_source_concept_id'] = long_df['measurement_concept_id']

        # Map observation info by person_id (from obs_map)
        long_df['measurement_event_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('measurement_event_id'))
        if "measurement_date" in person_map:
            long_df['measurement_date'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('measurement_date'))
        else:
            long_df['measurement_date'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('observation_date'))

        long_df['measurement_datetime'] = long_df['measurement_date']

        # Map measurement_type_concept_id, meas_event_field_concept_id by person_source_value (from person_map)
        long_df['measurement_type_concept_id'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('measurement_type_concept_id'))
        long_df['meas_event_field_concept_id'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('meas_event_field_concept_id'))

        # Map visit_detail_id and visit_occurrence_id and provider_id by person_id (from obs_map)
        long_df['visit_occurrence_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('visit_occurrence_id'))
        long_df['visit_detail_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('visit_detail_id'))
        long_df['provider_id'] = long_df['person_id'].map(lambda pid: obs_map.get(pid, {}).get('provider_id'))

        # Default/fixed fields
        # Map the remaining value from person_source_value (from person_map)
        long_df['measurement_time'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('measurement_time'))
        long_df['operator_concept_id'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('operator_concept_id'))
        long_df['range_low'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('range_low'))
        long_df['range_high'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('range_high'))
        long_df['unit_concept_id'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('unit_concept_id'))
        long_df['unit_source_value'] = long_df['person_source_value'].map(lambda psv: person_map.get(psv, {}).get('unit_source_value'))

        omop_cols = [
            'measurement_id', 'person_id', 'measurement_concept_id', 'measurement_date',
            'measurement_datetime', 'measurement_time', 'measurement_type_concept_id',
            'operator_concept_id', 'value_as_number', 'value_as_concept_id',
            'unit_concept_id', 'range_low', 'range_high', 'provider_id',
            'visit_occurrence_id', 'visit_detail_id', 'measurement_source_value',
            'measurement_source_concept_id', 'unit_source_value', 'value_source_value',
            'measurement_event_id', 'meas_event_field_concept_id'
        ]
        # Exclude rows if measurement_concept_id is NA or None, then fill up measurement_id if applicable
        long_df = long_df[long_df['measurement_concept_id'].notna()]
        if start_index is not None:
            long_df['measurement_id'] = range(start_index, start_index + len(long_df))
        else:
            long_df['measurement_id'] = None

        # ----------------- FACT RELATIONSHIP CONSTRUCTION ------------------
        # Build specimen_map from specimen_df (person_id ? specimen_id)
        specimen_map = dict(zip(specimen_df['person_id'], specimen_df['specimen_id']))

        # Only keep rows where specimen_id is found
        long_df['specimen_id'] = long_df['person_id'].map(specimen_map)
        long_df = long_df[long_df['specimen_id'].notna()].copy()

        # Generate fact_relationship entries
        # 1147330 = Measurement, 1147306 = Specimen
        # 32668 = Measurement to Specimen, 32669 = Specimen to Measurement
        fr_measure_to_spec = pd.DataFrame({
            'domain_concept_id_1': 1147330,
            'fact_id_1': long_df['measurement_id'],
            'domain_concept_id_2': 1147306,
            'fact_id_2': long_df['specimen_id'],
            'relationship_concept_id': 32668
        })

        fr_spec_to_measure = pd.DataFrame({
            'domain_concept_id_1': 1147306,
            'fact_id_1': long_df['specimen_id'],
            'domain_concept_id_2': 1147330,
            'fact_id_2': long_df['measurement_id'],
            'relationship_concept_id': 32669
        })

        fact_relationship_df = pd.concat([fr_measure_to_spec, fr_spec_to_measure], ignore_index=True)

        return long_df[omop_cols], fact_relationship_df


    def close(self):
        self.connection.close()
        self.engine.dispose()

class ODArgumentParser (argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"Error: {message}\n\n")
        self.print_help()
        sys.exit(2)



