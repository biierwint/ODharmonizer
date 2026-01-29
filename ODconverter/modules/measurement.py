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
This script is part of the ODharmonizer-v1.0 > ODconverter (streamlit).
It contains the function for measurement step.
'''

import streamlit as st
import pandas as pd
import os
import tempfile
import subprocess
from pathlib import Path
from utils.s3_utils import get_s3_fs, validate_s3_path_exists, validate_s3_output_path

def run_gene_expression_step(df_obs, person_path, specimen_path, enable_download):
    #st.header("Step 3: Create Measurement Table (Gene Expression)")

    input_method = st.radio("Input method for gene expression file", ["Upload", "S3 URL", "Local Path"])
    if input_method == "Upload":
        gex_file = st.file_uploader("Upload gene expression data (genes x samples)", type="csv")
    elif input_method == "S3 URL":
        gex_s3_url = st.text_input("S3 URL to gene expression data", placeholder="s3://bucket/path/gex.csv")
    elif input_method == "Local Path":
        gex_local_path = st.text_input("Enter full local path to GEX data", value="/odconverter/input/annotated-gex.csv")

    meas_event_field_concept_id = st.text_input("Meas Event Field Concept ID", "1147165")
    unit_concept_id = st.text_input("Unit Concept ID", "37533750")
    unit_source_value = st.text_input("Unit Source Value", "TRANSCRIPTS PER MILLION FORMULA")
    measurement_type_concept_id = st.text_input("Measurement Type Concept ID", "32856")
    start_index = st.number_input("Start Measurement ID from", value=1, step=1)

    output_method = st.radio("Output destination for output files", ["Local", "S3 URL"])
    output_path = st.text_input("Full output path for measurement.csv", value="output/measurement.csv")
    fact_output_path = st.text_input("Full output path for fact_relationship.csv", value="output/fact_relationship.csv")

    if st.button("Generate Measurement Table"):
        with tempfile.TemporaryDirectory() as tmpdir:
            obs_path = os.path.join(tmpdir, "observation.csv")
            gex_path = os.path.join(tmpdir, "gex.csv")
            out_path = os.path.join(tmpdir, "measurement.csv")
            fact_path = os.path.join(tmpdir, "fact_relationship.csv")
            person_tmp = os.path.join(tmpdir, "person.csv")
            specimen_tmp = os.path.join(tmpdir, "specimen.csv")

            df_obs.to_csv(obs_path, index=False)
            pd.read_csv(person_path).to_csv(person_tmp, index=False)
            pd.read_csv(specimen_path).to_csv(specimen_tmp, index=False)

            s3 = get_s3_fs()

            if input_method == "Upload" and gex_file:
                with open(gex_path, "wb") as f:
                    f.write(gex_file.read())
            elif input_method == "S3 URL":
                valid, msg = validate_s3_path_exists(gex_s3_url, s3)
                if not valid:
                    st.error(f"❌ Invalid GEX S3 input: {msg}")
                    st.stop()
                s3.get(gex_s3_url.replace("s3://", ""), gex_path)
            elif input_method == "Local Path":
                if not os.path.exists(gex_local_path):
                    st.error("❌ Local GEX file not found.")
                    st.stop()
                gex_path = gex_local_path
            else:
                st.error("❌ Invalid GEX input.")
                st.stop()

            cmd = [
                "python", "expression2measurement.py",
                "--person", person_tmp,
                "--in", gex_path,
                "--obs", obs_path,
                "--specimen", specimen_tmp,
                "--out", out_path,
                "--outfact", fact_path,
                "--mefid", meas_event_field_concept_id,
                "--uid", unit_concept_id,
                "--uvalue", unit_source_value,
                "--mtid", measurement_type_concept_id,
                "--start", str(start_index)
            ]

            st.text("Running expression2measurement.py...")
            try:
                subprocess.run(cmd, check=True)
                df_meas = pd.read_csv(out_path)
                df_fact = pd.read_csv(fact_path)

                if output_method == "Local":
                    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                    df_meas.to_csv(output_path, index=False)
                    df_fact.to_csv(fact_output_path, index=False)
                    st.success(f"✅ Saved locally to {output_path} and {fact_output_path}")
                    if enable_download:
                        with open(output_path, "rb") as f:
                            st.download_button("Download Measurement Table", f, file_name="measurement.csv")
                        with open(fact_output_path, "rb") as f:
                            st.download_button("Download Fact Relationship", f, file_name="fact_relationship.csv")
                else:
                    valid, msg = validate_s3_output_path(output_path, s3)
                    if not valid:
                        st.error(f"❌ Invalid S3 output path: {msg}")
                        st.stop()
                    s3.put(out_path, output_path.replace("s3://", ""))
                    s3.put(fact_path, fact_output_path.replace("s3://", ""))
                    st.success(f"✅ Uploaded to S3: {output_path} and {fact_output_path}")

                st.dataframe(df_meas.head(5))
                st.dataframe(df_fact.head(5))

            except subprocess.CalledProcessError:
                st.error("❌ Failed to generate GEX measurement table.")


def run_genomic_step(df_obs, person_path, specimen_path, enable_download):
    #st.header("Step 3: Create Measurement Table from Genomic Data (VCF)")

    input_method = st.selectbox("Input method for annotated VCF file", ["Upload", "S3 URL", "Local Path"])
    if input_method == "Upload":
        vcf_file = st.file_uploader("Upload OMOP-annotated VCF file", type=["vcf", "gz"])
    elif input_method == "S3 URL":
        vcf_s3_url = st.text_input("S3 URL to annotated VCF", placeholder="s3://bucket/path/input.vcf")
    elif input_method == "Local Path":
        vcf_local_path = st.text_input("Enter full local path to VCF", value="/absolute/path/to/input.vcf")

    meas_event_field_concept_id = st.text_input("Meas Event Field Concept ID", "1147165")
    measurement_type_concept_id = st.text_input("Measurement Type Concept ID", "32856")
    start_index = st.number_input("Start Measurement ID from", value=1, step=1)

    output_method = st.selectbox("Output destination for output files", ["Local", "S3 URL"], index=0)
    output_path = st.text_input("Full output path for measurement.csv", value="output/measurement.csv")
    fact_output_path = st.text_input("Full output path for fact_relationship.csv", value="output/fact_relationship.csv")

    if st.button("Generate Measurement Table"):
        with tempfile.TemporaryDirectory() as tmpdir:
            obs_path = os.path.join(tmpdir, "observation.csv")
            vcf_path = os.path.join(tmpdir, "input.vcf")
            out_path = os.path.join(tmpdir, "measurement.csv")
            fact_path = os.path.join(tmpdir, "fact_relationship.csv")
            person_tmp = os.path.join(tmpdir, "person.csv")
            specimen_tmp = os.path.join(tmpdir, "specimen.csv")

            df_obs.to_csv(obs_path, index=False)
            pd.read_csv(person_path).to_csv(person_tmp, index=False)
            pd.read_csv(specimen_path).to_csv(specimen_tmp, index=False)

            s3 = get_s3_fs()

            if input_method == "Upload" and vcf_file:
                with open(vcf_path, "wb") as f:
                    f.write(vcf_file.read())
            elif input_method == "S3 URL":
                valid, msg = validate_s3_path_exists(vcf_s3_url, s3)
                if not valid:
                    st.error(f"❌ Invalid VCF S3 input: {msg}")
                    st.stop()
                s3.get(vcf_s3_url.replace("s3://", ""), vcf_path)
            elif input_method == "Local Path":
                if not os.path.exists(vcf_local_path):
                    st.error("❌ Local VCF file not found.")
                    st.stop()
                vcf_path = vcf_local_path
            else:
                st.error("❌ Invalid input.")
                st.stop()

            cmd = [
                "python", "vcf2measurement.py",
                "--person", person_tmp,
                "--in", vcf_path,
                "--obs", obs_path,
                "--specimen", specimen_tmp,
                "--out", out_path,
                "--outfact", fact_path,
                "--mefid", meas_event_field_concept_id,
                "--mtid", measurement_type_concept_id,
                "--start", str(start_index)
            ]

            st.text("Running vcf2measurement.py...")
            try:
                subprocess.run(cmd, check=True)
                df_meas = pd.read_csv(out_path)
                df_fact = pd.read_csv(fact_path)

                if output_method == "Local":
                    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                    df_meas.to_csv(output_path, index=False)
                    df_fact.to_csv(fact_output_path, index=False)
                    st.success(f"✅ Saved locally to {output_path} and {fact_output_path}")
                    if enable_download:
                        with open(output_path, "rb") as f:
                            st.download_button("Download Measurement Table", f, file_name="measurement.csv")
                        with open(fact_output_path, "rb") as f:
                            st.download_button("Download Fact Relationship", f, file_name="fact_relationship.csv")
                else:
                    valid, msg = validate_s3_output_path(output_path, s3)
                    if not valid:
                        st.error(f"❌ Invalid S3 output path: {msg}")
                        st.stop()
                    s3.put(out_path, output_path.replace("s3://", ""))
                    s3.put(fact_path, fact_output_path.replace("s3://", ""))
                    st.success(f"✅ Uploaded to S3: {output_path} and {fact_output_path}")

                st.dataframe(df_meas.head(5))
                st.dataframe(df_fact.head(5))

            except subprocess.CalledProcessError:
                st.error("❌ Failed to generate genomic measurement table.")

