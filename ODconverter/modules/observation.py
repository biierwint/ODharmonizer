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
It contains the function for create observation step.
'''

import streamlit as st
import pandas as pd
import os
import tempfile
import subprocess
from pathlib import Path
from utils.s3_utils import get_s3_fs, validate_s3_path_exists, validate_s3_output_path

def run_observation_step(enable_download):
    #st.header("Step 1: Create Observation Table")

    input_method = st.selectbox("Input method for input file", ["Upload", "S3 URL", "Local Path"], index=0)
    person_file, s3_url, local_path = None, None, None
    if input_method == "Upload":
        person_file = st.file_uploader("Upload person_source_value CSV file", type="csv")
    elif input_method == "S3 URL":
        s3_url = st.text_input("S3 URL to person_source_value file", placeholder="s3://bucket/path/file.csv")
    elif input_method == "Local Path":
        local_path = st.text_input("Enter full local person_source_value CSV file path", value="/odconverter/input/file.csv")

    obs_concept_id = st.text_input("Observation Concept ID", "21495062")
    obs_type_concept_id = st.text_input("Observation Type Concept ID", "32856")
    value_concept_id = st.text_input("Value As Concept ID", "42531068")
    value_source_value = st.text_input("Value Source Value", "Gene Expression Array")
    prefix = st.text_input("Observation ID Prefix (optional)", "1000")

    output_method = st.selectbox("Output destination for observation.csv", ["Local", "S3 URL"], index=0)
    output_path = st.text_input("Full output path (e.g., output/observation.csv or s3://bucket/observation.csv)", value="output/observation.csv")

    if st.button("Generate Observation Table"):
        tmpdir = "tmp/observation_temp"
        os.makedirs(tmpdir, exist_ok=True)

        person_path = os.path.join(tmpdir, "person.csv")
        obs_path = os.path.join(tmpdir, "observation.csv")

        s3 = get_s3_fs()

        if input_method == "Upload" and person_file:
            with open(person_path, "wb") as f:
                f.write(person_file.read())
        elif input_method == "S3 URL":
            valid, msg = validate_s3_path_exists(s3_url, s3)
            if not valid:
                st.error(f"❌ Invalid S3 input path: {msg}")
                return
            s3.get(s3_url.replace("s3://", ""), person_path)
        elif input_method == "Local Path":
            # Normalize path
            local_path = os.path.abspath(local_path)
            st.text(f"Checking file: {local_path}")  # Debug info

            # Check existence
            if not os.path.exists(local_path):
                st.error(f"? File not found at specified local path: {local_path}")
                return

            # Check read permission
            if not os.access(local_path, os.R_OK):
                st.error(f"? File exists but is not readable (permission issue). "
                         f"Check your file permissions for {local_path}")
                return

            # File exists and is readable
            person_path = local_path
        else:
            st.error("❌ Invalid input source.")
            return

        cmd_obs = [
            "python", "create-observation.py",
            "--in", person_path,
            "--out", obs_path,
            "--cid", obs_concept_id,
            "--otid", obs_type_concept_id,
            "--vid", value_concept_id,
            "--vsource", value_source_value
        ]
        if prefix:
            cmd_obs.extend(["--prefix", prefix])

        st.text("Running create-observation.py...")
        try:
            subprocess.run(cmd_obs, check=True)
            print (cmd_obs)
            df_obs = pd.read_csv(obs_path)
            st.session_state["observation_df"] = df_obs
            st.session_state["person_path"] = person_path

            if output_method == "Local":
                local_path = Path(output_path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                df_obs.to_csv(local_path, index=False)
                st.success(f"✅ Saved locally to {local_path}")
                if enable_download:
                    with open(local_path, "rb") as f:
                        st.download_button("Download Observation Table", f, file_name=local_path.name)
            else:
                valid, msg = validate_s3_output_path(output_path, s3)
                if not valid:
                    st.error(f"❌ Invalid S3 output path: {msg}")
                    return
                with s3.open(output_path.replace("s3://", ""), "w") as f:
                    df_obs.to_csv(f, index=False)
                st.success(f"✅ Uploaded to {output_path}")

            st.dataframe(df_obs.head(5))
        except subprocess.CalledProcessError:
            st.error("❌ Failed to generate observation table.")

