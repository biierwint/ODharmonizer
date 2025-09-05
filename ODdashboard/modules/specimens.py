import streamlit as st
import plotly.express as px
from db import run_query

def show(engine, schema):
    st.header("4. Specimens")

    # --- Query specimens with concept_name ---
    sql = f"""
    SELECT s.specimen_concept_id,
           c.concept_name,
           COUNT(*) AS count
    FROM {schema}.specimen s
    JOIN {schema}.concept c
      ON s.specimen_concept_id = c.concept_id
    GROUP BY s.specimen_concept_id, c.concept_name
    ORDER BY count DESC
    LIMIT 10;
    """
    df = run_query(engine, sql)

    # --- Show table first ---
    st.subheader("Top 10 Specimen Types (Table)")
    st.dataframe(df, use_container_width=True)

    # --- Then show bar chart ---
    st.subheader("Top 10 Specimen Types (Chart)")
    st.plotly_chart(
        px.bar(
            df,
            x="concept_name",
            y="count",
            title="Specimen Type Distribution",
        ),
        use_container_width=True
    )
