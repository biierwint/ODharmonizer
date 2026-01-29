import streamlit as st
import os

from db import get_engine
import modules.demographics as demographics
import modules.measurements as measurements
import modules.observations as observations
import modules.specimens as specimens
import modules.relationships as relationships

os.environ["STREAMLIT_BROWSER_GATHERUSAGESTATS"] = "false"
st.set_page_config(page_title="OMOP CDM Dashboard", layout="wide")

# Initialize session state
if "engine" not in st.session_state:
    st.session_state.engine = None
if "schema" not in st.session_state:
    st.session_state.schema = None

# ---------------------------
# Sidebar
# ---------------------------
sidebar_placeholder = st.sidebar.empty()  # placeholder to dynamically overwrite sidebar

if st.session_state.engine is None:
    with sidebar_placeholder.container():
        st.sidebar.title("Database Connection")
        db_host = st.sidebar.text_input("Host", "localhost")
        db_port = st.sidebar.text_input("Port", "25432")
        db_name = st.sidebar.text_input("Database", "odmapper")
        db_schema = st.sidebar.text_input("Schema", "cdm_odmapper")
        db_user = st.sidebar.text_input("User", "postgres")
        db_pass = st.sidebar.text_input("Password", type="password")

        if st.sidebar.button("Connect"):
            try:
                engine = get_engine(db_user, db_pass, db_host, db_port, db_name, db_schema)
                st.session_state.engine = engine
                st.session_state.schema = db_schema
                st.success("✅ Connected to OMOP database")
                st.rerun()  # rerun so sidebar can update
            except Exception as e:
                st.error(f"❌ Connection failed: {e}")
else:
    # Overwrite sidebar placeholder with connected info
    with sidebar_placeholder.container():
        st.success(f"✅ Connected to DB (schema: {st.session_state.schema})")
        # Optional: Disconnect button
        if st.button("Disconnect"):
            st.session_state.engine = None
            st.session_state.schema = None
            st.rerun()

    # ---------------------------
    # Main navigation
    # ---------------------------
    page = st.radio("Navigate", [
        "Demographics",
        "Measurements",
        "Observations",
        "Specimens",
        "Relationships"
    ], horizontal=True)  # horizontal menu

    if page == "Demographics":
        demographics.show(st.session_state.engine, st.session_state.schema)
    elif page == "Measurements":
        measurements.show(st.session_state.engine, st.session_state.schema)
    elif page == "Observations":
        observations.show(st.session_state.engine, st.session_state.schema)
    elif page == "Specimens":
        specimens.show(st.session_state.engine, st.session_state.schema)
    elif page == "Relationships":
        relationships.show(st.session_state.engine, st.session_state.schema)

