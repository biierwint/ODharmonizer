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
It contains the function for create specimen step.
'''

import streamlit as st
import pandas as pd
import os
import tempfile
import subprocess
from pathlib import Path
from utils.s3_utils import get_s3_fs, validate_s3_output_path

def run_specimen_step(enable_download):
    st.success("✅ Observation table and person source available")

    if "observation_df" not in st.session_state or "person_path" not in st.session_state:
        st.warning("⚠️ Observation table or person_path missing. Please complete Step 1.")
        return

    df_obs = st.session_state["observation_df"]
    person_path = st.session_state["person_path"]

    # Optional parameters
    specimen_concept_id = st.text_input("Specimen Concept ID", "4047495")
    specimen_type_concept_id = st.text_input("Specimen Type Concept ID", "32856")
    prefix = st.text_input("Specimen ID Prefix (optional, integer value)", "1000")

    output_method = st.selectbox("Output destination for specimen.csv", ["Local", "S3 URL"], index=0)
    output_path = st.text_input("Full output path for specimen.csv", value="output/specimen.csv")

    if st.button("Generate Specimen Table"):
        with tempfile.TemporaryDirectory() as tmpdir:
            person_path = st.session_state["person_path"]
            specimen_path = os.path.join(tmpdir, "specimen.csv")

            cmd_specimen = [
                "python", "create-specimen.py",
                "--in", person_path,
                "--out", specimen_path
            ]

            if specimen_concept_id:
                cmd_specimen.extend(["--specimen", specimen_concept_id])
            if specimen_type_concept_id:
                cmd_specimen.extend(["--sctid", specimen_type_concept_id])
            if prefix:
                cmd_specimen.extend(["--prefix", prefix])

            st.text("Running create-specimen.py...")
            try:
                subprocess.run(cmd_specimen, check=True)
                df_specimen = pd.read_csv(specimen_path)

                # Save output
                if output_method == "Local":
                    local_path = Path(output_path)
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    df_specimen.to_csv(local_path, index=False)
                    st.session_state["specimen_path"] = local_path
                    st.session_state["specimen_df"] = df_specimen

                    st.success(f"✅ Saved locally to {local_path}")
                    if enable_download:
                        with open(local_path, "rb") as f:
                            st.download_button("Download Specimen Table", f, file_name=local_path.name)
                else:
                    s3 = get_s3_fs()
                    valid, msg = validate_s3_output_path(output_path, s3)
                    if not valid:
                        st.error(f"❌ Invalid S3 output path: {msg}")
                        st.stop()
                    with s3.open(output_path.replace("s3://", ""), "w") as f:
                        df_specimen.to_csv(f, index=False)
                        st.session_state["specimen_path"] = output_path
                        st.session_state["specimen_df"] = df_specimen
                    st.success(f"✅ Uploaded to {output_path}")

                st.dataframe(df_specimen.head(5))

            except subprocess.CalledProcessError:
                st.error("❌ Failed to generate specimen table.")
