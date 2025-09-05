import streamlit as st
from db import run_query

def show(engine, schema):
    st.header("5. Fact Relationships")

    sql = f"""
    SELECT domain_concept_id_1, domain_concept_id_2, relationship_concept_id, COUNT(*) AS count
    FROM {schema}.fact_relationship
    GROUP BY domain_concept_id_1, domain_concept_id_2, relationship_concept_id
    ORDER BY count DESC
    LIMIT 10;
    """
    df = run_query(engine, sql)
    st.dataframe(df)

