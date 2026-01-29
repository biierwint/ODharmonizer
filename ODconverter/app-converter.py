import os
os.environ["STREAMLIT_BROWSER_GATHERUSAGESTATS"] = "false"

import streamlit as st
from modules.observation import run_observation_step
from modules.specimen import run_specimen_step
from modules.measurement import run_gene_expression_step, run_genomic_step

st.set_page_config(page_title="Genomic / Expression Data to OMOP CDM", layout="wide")
st.title("ODconverter: Convert Omics Data to OMOP CDM Measurement")

enable_download = os.getenv("ENABLE_FILE_DOWNLOAD", "true").lower() == "true"

# Step 0: Select Data Type
st.markdown("### Type of Analysis")
analysis_type = st.radio("Select Data Type", ["Expression Data", "Genomic Data"])

# Step 1: Create Specimen Table
if analysis_type:
    st.markdown("### Step 1: Create Specimen Table")
    run_specimen_step(enable_download)

# Step 2: Create Observation Table
if "specimen_path" in st.session_state and "person_path" in st.session_state:
    st.markdown("---")
    st.markdown("### Step 2: Create Observation Table")
    run_observation_step(enable_download)

# Step 3: Create Measurement Table
if all(k in st.session_state for k in ["observation_df", "person_path", "specimen_path"]):
    st.markdown("---")
    st.markdown("### Step 3: Create Measurement Table")

    if analysis_type == "Expression Data":
        run_gene_expression_step(
            st.session_state["observation_df"],
            st.session_state["person_path"],
            st.session_state["specimen_path"],
            enable_download
        )
    elif analysis_type == "Genomic Data":
        run_genomic_step(
            st.session_state["observation_df"],
            st.session_state["person_path"],
            st.session_state["specimen_path"],
            enable_download
        )
