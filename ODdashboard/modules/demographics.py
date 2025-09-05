import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sqlalchemy import text

def show(engine, schema):
    st.title("Demographics Dashboard")

    # --- Pull person table ---
    sql = text(f"""
        SELECT person_id, gender_concept_id, year_of_birth,
               race_concept_id, ethnicity_concept_id
        FROM {schema}.person
    """)
    df = pd.read_sql(sql, engine)

    if df.empty:
        st.warning("No person data available.")
        return

    df["age"] = 2025 - df["year_of_birth"]

    concept_ids = pd.unique(
        df[["gender_concept_id", "race_concept_id", "ethnicity_concept_id"]].values.ravel()
    )
    concept_ids = [int(x) for x in concept_ids if pd.notna(x)]

    if concept_ids:
        # Build SQL with ARRAY literal
        ids_str = ",".join(map(str, concept_ids))  # "8532,38003587,0,..."
        concept_sql = f"""
            SELECT concept_id, concept_name
            FROM {schema}.concept
            WHERE concept_id = ANY(ARRAY[{ids_str}])
        """
        df_concepts = pd.read_sql(concept_sql, engine)
        id2name = dict(zip(df_concepts["concept_id"], df_concepts["concept_name"]))
    else:
        id2name = {}

    # Map with fallback
    df["gender"] = df["gender_concept_id"].map(id2name).fillna("Unknown")
    df["race"] = df["race_concept_id"].map(id2name).fillna("Unknown")
    df["ethnicity"] = df["ethnicity_concept_id"].map(id2name).fillna("Unknown")

    # --- 1. Summary Table ---
    st.subheader("Summary Statistics")

    total_subjects = len(df)

    gender_counts = df["gender"].value_counts().reset_index()
    gender_counts.columns = ["gender", "count"]

    race_counts = df["race"].value_counts().reset_index()
    race_counts.columns = ["race", "count"]

    ethnicity_counts = df["ethnicity"].value_counts().reset_index()
    ethnicity_counts.columns = ["ethnicity", "count"]

    age_mean = df["age"].mean()
    age_std = df["age"].std()

    age_lower = max(0, age_mean - 2 * age_std)  # prevent negative ages
    age_upper = age_mean + 2 * age_std
    age_summary = f"{age_mean:.1f} ({age_lower:.1f} - {age_upper:.1f})"
    
    
    summary_data = {
        "Metric": [
            "Total Subjects",
            "By Gender",
            "By Race",
            "Age (Mean +/- 2SD)"
        ],
        "Value": [
            total_subjects,
            ", ".join([f"{row['gender']}: {row['count']}" for _, row in gender_counts.iterrows()]),
            ", ".join([f"{row['race']}: {row['count']}" for _, row in race_counts.iterrows()]),
            age_summary
        ]
    }

    summary_df = pd.DataFrame(summary_data)
    summary_df = summary_df.astype(str)
    st.dataframe(summary_df, use_container_width=True)

    # --- 2. Charts ---
    st.subheader("Visualizations")

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            px.histogram(df, x="age", nbins=30, title="Age Distribution"),
            use_container_width=True
        )
    with col2:
        st.plotly_chart(
            px.pie(
                gender_counts,              # Use the counts table
                names="gender",             # Category
                values="count",             # Numeric count
                title="Gender Distribution"
            ),
            use_container_width=True
    )

    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(
            px.pie(race_counts, names="race", values="count", title="Race Distribution"),
            use_container_width=True
        )
