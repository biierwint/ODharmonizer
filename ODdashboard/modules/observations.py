import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text

def show(engine, schema):
    st.header("Observation Dashboard")

    # --- Distribution: how many subjects have X observations ---
    query_counts = text(f"""
        SELECT person_id, COUNT(*) as num_observations
        FROM {schema}.observation
        GROUP BY person_id
    """)
    df_counts = pd.read_sql(query_counts, engine)

    # Compute distribution table safely
    df_summary = (
        df_counts["num_observations"]
        .value_counts()
        .rename_axis("num_observations")  # index becomes column
        .reset_index(name="num_subjects")  # series values become 'num_subjects'
        .sort_values("num_observations")
        .reset_index(drop=True)
    )

    st.subheader("Table: Number of subjects by observation count")
    st.dataframe(df_summary, use_container_width=True)

    
    # Plot histogram
    fig_counts = px.histogram(
        df_counts,
        x="num_observations",
        nbins=50,
        title="Distribution of observation counts per subject"
    )
    st.plotly_chart(fig_counts, use_container_width=True)


